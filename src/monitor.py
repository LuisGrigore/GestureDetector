import time
from threading import Thread
from queue import Queue
from typing import Optional
from worker_pool import WorkerPool
from context import BatchContext
from configuration import FailurePolicy
from worker_errors import WorkerFatalError


class WorkerMonitor:
    def __init__(self, pool: WorkerPool, ctx: BatchContext):
        self.pool = pool
        self.ctx = ctx
        self.events: Queue[Exception] = Queue()
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

            if self.ctx.config.on_worker_death == FailurePolicy.RESTART:
                self.pool.restart_dead()

            time.sleep(self.ctx.config.worker_monitoring_frequency)
