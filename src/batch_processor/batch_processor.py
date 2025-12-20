from abc import abstractmethod
from queue import Empty
from typing import Callable, Generic, TypeVar, Optional, List
from contextlib import AbstractContextManager

from context import ControlContext
from .context import BatchProcessorContext
from monitor.configuration import MonitorConfig
from .worker_reported_error import WorkerReportedError
from .batch_worker import BatchWorkerExecutor, IBatchWorker
from worker_pool.worker_pool import WorkerPool
from monitor.monitor import WorkerMonitor, MonitorContext
from .configuration import (
    BatchProcessorConfig,
    SharedConfig,
    ProcessorConfig,
    FailurePolicy,
)
from .exception_info import ExceptionInfo


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


class BatchProcessor(IBatchProcessor[I, O]):
    def __init__(
        self, pool: WorkerPool, monitor: WorkerMonitor, ctx: BatchProcessorContext[I, O]
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
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()


def create_batch_processor(
    n_workers: int,
    worker_factory: Callable[[], IBatchWorker[I, O]],
    config: BatchProcessorConfig,
) -> IBatchProcessor[I, O]:

    shared_config = SharedConfig(logging=config.logging)
    processor_config = ProcessorConfig(
        shared=shared_config, on_worker_exception=config.on_worker_exception
    )
    monitor_config = MonitorConfig(
        shared=shared_config,
        on_worker_death=config.on_worker_death,
        worker_monitoring_frequency=config.worker_monitoring_frequency,
    )

    control_ctx = ControlContext()
    processor_ctx = BatchProcessorContext[I, O](processor_config, control_ctx)

    def executor_factory():
        return BatchWorkerExecutor[I, O](processor_ctx, worker_factory)

    pool = WorkerPool(
        n_workers=n_workers,
        worker_factory=executor_factory,
        worker_timeout=config.worker_timeout,
    )

    monitor_ctx = MonitorContext(monitor_config, control_ctx)

    monitor = WorkerMonitor[I, O](pool, monitor_ctx)

    return BatchProcessor[I, O](pool, monitor, processor_ctx)
