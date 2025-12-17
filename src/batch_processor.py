from queue import Empty
from typing import Callable, Generic, TypeVar, Optional, List
from contextlib import AbstractContextManager
from context import BatchContext
from worker import BatchWorkerExecutor, IBatchWorker
from worker_pool import WorkerPool
from monitor import WorkerMonitor
from configuration import BatchProcessorConfig, FailurePolicy
from exception_info import ExceptionInfo
from worker_errors import WorkerReportedError, WorkerFatalError


I = TypeVar("I")
O = TypeVar("O")


class BatchProcessor(Generic[I, O], AbstractContextManager):
    def __init__(self, pool: WorkerPool, monitor: WorkerMonitor, ctx: BatchContext):
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

    def _handle_monitor_events(self) -> None:
        while not self.monitor.events.empty():
            event = self.monitor.events.get()
            if isinstance(event, WorkerFatalError):
                if self.ctx.config.on_worker_death == FailurePolicy.ABORT:
                    self._abort(event)

    def _abort(self, exc: Exception) -> None:
        if not self._fatal_exception:
            self._fatal_exception = exc
            self.ctx.abort_event.set()
            self.ctx.stop_event.set()

    def stop(self) -> None:
        self._handle_worker_exceptions()
        self._handle_monitor_events()

        self.ctx.stop_event.set()
        self.monitor.stop()
        self.pool.stop()
        self.pool.cleanup()

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()

def create_batch_processor(
    n_workers: int,
    worker_factory: Callable[[], IBatchWorker[I, O]],
    config: BatchProcessorConfig,
    worker_timeout: Optional[float] = None,
) -> BatchProcessor[I, O]:

    ctx = BatchContext(config)

    def executor_factory():
        return BatchWorkerExecutor(ctx, worker_factory)

    pool = WorkerPool(
        n_workers=n_workers,
        worker_factory=executor_factory,
        worker_timeout=worker_timeout,
    )

    monitor = WorkerMonitor(pool, ctx)

    return BatchProcessor(pool, monitor, ctx)