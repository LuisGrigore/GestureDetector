from abc import ABC, abstractmethod
from queue import Empty
from typing import Callable, Generic, TypeVar
from .context import BatchProcessorContext
from .exception_info import ExceptionInfo
from ..logger import logger
from ..worker_pool.worker import IWorker


I = TypeVar("I")
O = TypeVar("O")


class IBatchWorker(Generic[I, O], ABC):
    @abstractmethod
    def work(self, item: I) -> O:
        pass


class BatchWorkerExecutor(Generic[I, O], IWorker):
    def __init__(
        self,
        ctx: BatchProcessorContext[I, O],
        worker_factory: Callable[[], IBatchWorker[I, O]],
    ):
        self.ctx = ctx
        self.worker_factory = worker_factory

    def target(self) -> None:
        worker = self.worker_factory()

        while not self.ctx.stop_event.is_set():
            if self.ctx.abort_event.is_set():
                break

            try:
                item = self.ctx.in_queue.get(timeout=0.1)
            except Empty:
                continue

            try:
                result = worker.work(item)
                self.ctx.out_queue.put(result)

            except Exception as exc:
                info = ExceptionInfo.from_exception(exc, item)
                self.ctx.error_queue.put(info)

                if self.ctx.config.shared.logging:
                    logger.exception("Worker exception")
