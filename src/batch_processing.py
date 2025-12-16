from __future__ import annotations

from abc import ABC, abstractmethod
from multiprocessing import Event
from multiprocessing.synchronize import Event as EventType
from queue import Empty
from typing import Callable, Generic, Optional, TypeVar, List
from contextlib import AbstractContextManager
from threading import Lock
from gen_mp_queue import GenMPQueue
from worker_pool import IWorkerPool, WorkerPool
from monitor import WorkerMonitor
from configuration import BatchProcessorConfig
from worker import IWorker
from exception_info import ExceptionInfo
from logger import logger

I = TypeVar("I")
O = TypeVar("O")



class IBatchProcessor(Generic[I, O], AbstractContextManager, ABC):
    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def stop(self) -> None:
        pass



class BatchProcessor(IBatchProcessor[I, O]):
    def __init__(
        self,
        in_queue: GenMPQueue[I],
        out_queue: GenMPQueue[O],
        error_queue: GenMPQueue[ExceptionInfo],
        worker_generator: Callable[[], IWorker[I, O]],
        config: BatchProcessorConfig,
    ) -> None:
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.error_queue = error_queue
        self.worker_generator = worker_generator
        self.config = config
        self.stop_event: EventType = Event()

        self.workers_lock = Lock()
        self.worker_pool: IWorkerPool = WorkerPool(
            config.n_workers,
            self._worker_target,
            config.worker_timeout,
            self.workers_lock,
        )

        self.abort_event: Optional[EventType] = None
        if config.stop_on_worker_death or config.stop_on_reported_exception:
            self.abort_event = Event()

        self._exception_to_raise: Optional[Exception] = None

        self.worker_monitor = WorkerMonitor(
            self.worker_pool,
            self.error_queue,
            self.stop_event,
            self.abort_event,
            self.config,
            self._exception_to_raise,
        )

    # ------------------ Worker Target ------------------
    def _worker_target(self) -> None:
        worker = self.worker_generator()
        while not self.stop_event.is_set() or not self.in_queue.empty():
            if self.abort_event and self.abort_event.is_set():
                break
            try:
                item = self.in_queue.get(timeout=0.001)
            except Empty:
                continue
            try:
                result = worker.work(item)
                self.out_queue.put(result)
            except SystemExit:
                if self.abort_event and self.config.stop_on_reported_exception:
                    self.abort_event.set()
                raise
            except Exception as exc:
                if self.abort_event and self.config.stop_on_reported_exception:
                    self.abort_event.set()
                info = ExceptionInfo.from_exception(exc, item)
                try:
                    self.error_queue.put_nowait(info)
                except Exception:
                    if self.config.logging:
                        logger.exception(
                            "Failed to put ExceptionInfo into error_queue; re-raising"
                        )
                    raise
                if self.config.logging:
                    logger.warning(
                        "Worker reported exception and continues: %s", info.exc_type
                    )

    # ------------------ Start/Stop ------------------
    def start(self) -> None:
        if self.worker_pool.get_worker_list():
            raise RuntimeError("BatchProcessor already started")
        self.stop_event.clear()
        if self.abort_event:
            self.abort_event.clear()
        self.worker_pool.start_workers()
        self.worker_monitor.start_monitoring()

    def stop(self) -> None:
        self.stop_event.set()
        self.worker_monitor.stop_monitoring()
        self.worker_pool.join_workers()
        self.worker_pool.cleanup_workers()
        if self._exception_to_raise:
            raise self._exception_to_raise

    # ------------------ Error Handling ------------------
    def poll_exceptions(self) -> List[ExceptionInfo]:
        infos: List[ExceptionInfo] = []
        while True:
            try:
                info = self.error_queue.get_nowait()
            except Empty:
                break
            else:
                infos.append(info)
        return infos

    # ------------------ Context ------------------
    def __enter__(self) -> BatchProcessor[I, O]:
        return self

    def __exit__(self, exc_type, exc_value, tb) -> None:
        try:
            self.stop()
        except Exception:
            if exc_type is None:
                raise
            else:
                if self.config.logging:
                    logger.exception("Error during shutdown")


