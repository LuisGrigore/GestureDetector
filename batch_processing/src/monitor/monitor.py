from abc import ABC, abstractmethod
import time
from threading import Thread
from queue import Queue
from typing import Optional, Generic, TypeVar
from .context import MonitorContext
from ..worker_pool.worker_pool import IWorkerPool
from ..configuration import FailurePolicy

I = TypeVar("I")
O = TypeVar("O")


class IWorkerMonitor(Generic[I, O], ABC):
    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def stop(self) -> None:
        pass


class WorkerMonitor(IWorkerMonitor):
    def __init__(self, pool: IWorkerPool, ctx: MonitorContext):
        self.pool = pool
        self.ctx = ctx
        self.events = Queue()
        self._thread: Optional[Thread] = None

    def start(self) -> None:
        self._thread = Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._thread:
            self._thread.join()

    def _loop(self) -> None:
        while not self.ctx.stop_event.is_set():
            if self.ctx.config.on_worker_death != FailurePolicy.IGNORE:
                for fatal in self.pool.fatal_errors():
                    self.events.put(fatal)
                    if self.ctx.config.on_worker_death == FailurePolicy.ABORT:
                        if not self.ctx.fatal_exception:
                            self.ctx.fatal_exception = fatal
                        self.ctx.abort_event.set()
                        self.ctx.stop_event.set()

            if self.ctx.config.on_worker_death == FailurePolicy.RESTART:
                self.pool.restart_dead()

            time.sleep(self.ctx.config.worker_monitoring_frequency)
