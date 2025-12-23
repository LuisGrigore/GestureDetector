from abc import abstractmethod
from queue import Empty
from typing import Generic, TypeVar, Optional, List
from contextlib import AbstractContextManager
from .context import BatchProcessorContext
from .worker_reported_error import WorkerReportedError
from .exception_info import ExceptionInfo
from ..configuration import FailurePolicy
from ..worker_pool.worker_pool import IWorkerPool
from ..monitor.monitor import IWorkerMonitor


I = TypeVar("I")
O = TypeVar("O")


class IBatchProcessor(Generic[I, O], AbstractContextManager):
    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def stop(self) -> None:
        pass

    @abstractmethod
    def poll_exceptions(self) -> List[ExceptionInfo]:
        pass

    @abstractmethod
    def put(self, item: I) -> None:
        pass

    @abstractmethod
    def get_nowait(self) -> O:
        pass
    
    @abstractmethod
    def get(self) -> O:
        pass


class BatchProcessor(IBatchProcessor[I, O]):
    def __init__(
        self, pool: IWorkerPool, monitor: IWorkerMonitor, ctx: BatchProcessorContext[I, O]
    ):
        self.pool = pool
        self.monitor = monitor
        self.ctx = ctx
        self._fatal_exception: Optional[Exception] = None

    def start(self) -> None:
        self.ctx.stop_event.clear()
        self.ctx.abort_event.clear()
        self.pool.start()
        self.monitor.start()

    def _handle_worker_exceptions(self) -> None:
        while True:
            try:
                info = self.ctx.error_queue.get_nowait()
            except Empty:
                break

            if self.ctx.config.on_worker_exception == FailurePolicy.ABORT:
                self._abort(WorkerReportedError(info))

    def _abort(self, exc: Exception) -> None:
        if not self._fatal_exception:
            self._fatal_exception = exc
            self.ctx.abort_event.set()
            self.ctx.stop_event.set()

    def stop(self) -> None:
        self._handle_worker_exceptions()

        self.ctx.stop_event.set()
        self.monitor.stop()
        self.pool.stop()
        self.pool.cleanup()

        if self.ctx.fatal_exception and not self._fatal_exception:
            self._fatal_exception = self.ctx.fatal_exception

        if self._fatal_exception:
            raise self._fatal_exception

    def poll_exceptions(self) -> List[ExceptionInfo]:
        infos = []
        while True:
            try:
                infos.append(self.ctx.error_queue.get_nowait())
            except Empty:
                break
        return infos

    def put(self, item: I) -> None:
        self.ctx.in_queue.put(item)

    def get(self) -> O:
        return self.ctx.out_queue.get()

    def get_nowait(self) -> O:
        return self.ctx.out_queue.get_nowait()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()
