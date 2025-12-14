from __future__ import annotations

from abc import ABC, abstractmethod
import traceback
from dataclasses import dataclass
from multiprocessing import Event, Process
from multiprocessing.synchronize import Event as EventType
from queue import Empty
from typing import Callable, Generic, Optional, TypeVar, List
from contextlib import AbstractContextManager
import logging
import time
from threading import Thread, Lock
from gen_mp_queue import GenMPQueue

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(processName)s] %(levelname)s - %(message)s",
)

I = TypeVar("I")
O = TypeVar("O")


@dataclass
class ExceptionInfo:
    exc_type: str
    message: str
    tb: str
    item_repr: str

    @classmethod
    def from_exception(cls, exc: Exception, item: I) -> "ExceptionInfo":
        return cls(
            exc_type=type(exc).__name__,
            message=str(exc),
            tb=traceback.format_exc(),
            item_repr=repr(item),
        )


class WorkerError(Exception):
    pass


class WorkerFatalError(WorkerError):
    def __init__(self, pid: int | None, exitcode: int | None):
        super().__init__(f"Worker pid={pid} died with exitcode={exitcode}")
        self.pid = pid
        self.exitcode = exitcode


class WorkerReportedError(WorkerError):
    def __init__(self, info: ExceptionInfo):
        super().__init__(
            f"Worker reported exception {info.exc_type}: {info.message}\n{info.tb}"
        )
        self.info = info


class IWorker(Generic[I, O], AbstractContextManager, ABC):
    def __init__(self, in_queue: GenMPQueue[I], out_queue: GenMPQueue[O]) -> None:
        self.in_queue: GenMPQueue[I] = in_queue
        self.out_queue: GenMPQueue[O] = out_queue

    @abstractmethod
    def work(self, item: I) -> O:
        pass

    def __enter__(self) -> IWorker[I, O]:
        return self

    def __exit__(self, exc_type, exc_value, tb) -> None:
        pass


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
        n_workers: int,
        worker_generator: Callable[[GenMPQueue[I], GenMPQueue[O]], IWorker[I, O]],
        worker_monitoring_frequency: float,
        worker_timeout: Optional[float] = None,
        stop_on_reported_exception: bool = False,
        stop_on_worker_death: bool = True,
        restart_dead_workers: bool = False,
        logging: bool = True,
    ) -> None:
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.error_queue = error_queue
        self.n_workers = n_workers
        self.worker_monitoring_frequency = worker_monitoring_frequency
        self.worker_generator = worker_generator
        self.stop_event: EventType = Event()

        self.workers: List[Process] = []
        self.worker_lock = Lock()
        self.worker_timeout = worker_timeout

        self.abort_event: Optional[EventType] = None
        self.stop_on_reported_exception = stop_on_reported_exception
        self.stop_on_worker_death = stop_on_worker_death
        if stop_on_worker_death or stop_on_reported_exception:
            self.abort_event = Event()

        self.restart_dead_workers = restart_dead_workers
        self.monitor_thread: Optional[Thread] = None
        self._exception_to_raise: Optional[Exception] = None

        self.logging = logging

    # ------------------ Worker Target ------------------
    def _worker_target(self) -> None:
        worker = self.worker_generator(self.in_queue, self.out_queue)
        while not self.stop_event.is_set() or not self.in_queue.empty():
            if self.abort_event and self.abort_event.is_set():
                break
            try:
                item = self.in_queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                result = worker.work(item)
                self.out_queue.put(result)
            except SystemExit:
                if self.abort_event and self.stop_on_reported_exception:
                    self.abort_event.set()
                raise
            except Exception as exc:
                if self.abort_event and self.stop_on_reported_exception:
                    self.abort_event.set()
                info = ExceptionInfo.from_exception(exc, item)
                try:
                    self.error_queue.put_nowait(info)
                except Exception:
                    if self.logging:
                        logger.exception(
                            "Failed to put ExceptionInfo into error_queue; re-raising"
                        )
                    raise
                if self.logging:
                    logger.warning(
                        "Worker reported exception and continues: %s", info.exc_type
                    )

    # ------------------ Start/Stop ------------------
    def start(self) -> None:
        if self.workers:
            raise RuntimeError("BatchProcessor already started")
        self.stop_event.clear()
        if self.abort_event:
            self.abort_event.clear()
        with self.worker_lock:
            for _ in range(self.n_workers):
                p = Process(target=self._worker_target)
                p.start()
                self.workers.append(p)
        if self.restart_dead_workers or self.stop_on_reported_exception or self.stop_on_worker_death:
            self.monitor_thread = Thread(target=self._monitor_loop, daemon=True)
            self.monitor_thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.monitor_thread:
            self.monitor_thread.join()
        self._join_workers()
        self._cleanup_workers()
        if self._exception_to_raise:
            raise self._exception_to_raise

    # ------------------ Monitor ------------------
    def _monitor_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                self._monitor_once()
            except Exception as exc:
                if not self._exception_to_raise:
                    self._exception_to_raise = exc
            time.sleep(self.worker_monitoring_frequency)

    def _monitor_once(self) -> None:
        with self.worker_lock:
            self._maybe_restart_dead_workers()
        self._check_error_queue()
        self._check_fatal_workers()

    # ------------------ Worker Management ------------------
    def _join_workers(self) -> None:
        with self.worker_lock:
            for p in self.workers:
                if self.worker_timeout:
                    p.join(timeout=self.worker_timeout)
                else:
                    p.join()

    def _cleanup_workers(self) -> None:
        with self.worker_lock:
            for p in self.workers:
                if p.is_alive():
                    try:
                        p.terminate()
                    except Exception:
                        pass
            self.workers.clear()

    def _maybe_restart_dead_workers(self) -> None:
        if not self.restart_dead_workers:
            return
        alive = [p for p in self.workers if p.is_alive()]
        dead_count = self.n_workers - len(alive)
        if dead_count <= 0:
            return
        if self.logging:
            logger.info("Detected %d dead workers; restarting...", dead_count)
        self.workers = alive
        for _ in range(dead_count):
            p = Process(target=self._worker_target)
            p.start()
            self.workers.append(p)

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

    def _check_error_queue(self) -> None:
        while True:
            try:
                info = self.error_queue.get_nowait()
            except Empty:
                break
            if self.logging:
                logger.error(
                    "Worker reported exception: %s: %s", info.exc_type, info.message
                )
            if self.stop_on_reported_exception:
                with self.worker_lock:
                    for p in self.workers:
                        if p.is_alive():
                            try:
                                p.terminate()
                            except Exception:
                                pass
                    self.workers.clear()
                raise WorkerReportedError(info)
            else:
                # Volver a poner en la cola para que el usuario pueda acceder
                self.error_queue.put(info)

    def _check_fatal_workers(self) -> None:
        for p in self.workers:
            if p.exitcode is not None and p.exitcode != 0:
                fatal = WorkerFatalError(p.pid, p.exitcode)
                if self.logging:
                    logger.error(
                        "Worker died unexpectedly: pid=%s exitcode=%s",
                        fatal.pid,
                        fatal.exitcode,
                    )
                if self.stop_on_worker_death:
                    if self.abort_event:
                        self.abort_event.set()
                    with self.worker_lock:
                        for p1 in self.workers:
                            p1.join(timeout=self.worker_timeout)
                        for p2 in self.workers:
                            if p2.is_alive():
                                try:
                                    p2.terminate()
                                except Exception:
                                    pass
                        self.workers.clear()
                    raise fatal

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
                if self.logging:
                    logger.exception("Error during shutdown")


"""
BatchProcessor con ejemplo de uso
"""
import sys
import time
from gen_mp_queue import GenMPQueue

# Suponiendo que IWorker y BatchProcessor ya están definidos


class ExampleWorker(IWorker[int, str]):
    def work(self, item: int) -> str:
        if item == 3:
            raise ValueError("Boom triggered")
        if item == 5:
            sys.exit(2)  # simulación de salida fatal
        return f"Processed {item}"


if __name__ == "__main__":
    in_q: GenMPQueue[int] = GenMPQueue()
    out_q: GenMPQueue[str] = GenMPQueue()
    err_q: GenMPQueue[ExceptionInfo] = GenMPQueue()

    # poner algunos items en la cola
    for i in range(1, 7):
        in_q.put(i)

    bp = BatchProcessor(
        in_queue=in_q,
        out_queue=out_q,
        error_queue=err_q,
        n_workers=2,
        worker_monitoring_frequency=0.1,
        worker_generator=lambda iq, oq: ExampleWorker(iq, oq),
        stop_on_reported_exception=False,
        stop_on_worker_death=False,
        restart_dead_workers=True,
    )

    bp.start()

    # dar tiempo a procesar
    time.sleep(0.1)

    try:
        bp.stop()
    except WorkerReportedError as wre:
        print("Parent caught reported error:", wre)
    except WorkerFatalError as wfe:
        print("Parent caught fatal worker death:", wfe)

    # mostrar resultados procesados
    while not out_q.empty():
        print("Result:", out_q.get())

    # mostrar errores reportados
    while not err_q.empty():
        info = err_q.get()
        print("Reported error:", info)
