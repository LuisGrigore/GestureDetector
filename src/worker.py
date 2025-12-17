from abc import ABC, abstractmethod
from queue import Empty
from typing import Callable, Generic, TypeVar
from context import BatchContext
from exception_info import ExceptionInfo
from logger import logger


I = TypeVar("I")
O = TypeVar("O")


class IWorker(ABC):
    @abstractmethod
    def target(self) -> None:
        pass


class IBatchWorker(Generic[I, O], ABC):
    @abstractmethod
    def work(self, item: I) -> O:
        pass


class BatchWorkerExecutor(Generic[I, O], IWorker):
    def __init__(
        self,
        ctx: BatchContext,
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

                if self.ctx.config.logging:
                    logger.exception("Worker exception")
