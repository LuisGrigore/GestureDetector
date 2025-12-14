from abc import ABC, abstractmethod
from multiprocessing import Event, Process, Queue
from os import getpid
from typing import Callable, Generic, TypeVar, Optional
from multiprocessing.synchronize import Event as EventType
from queue import Empty
from contextlib import AbstractContextManager
import logging
from gen_mp_queue import GenMPQueue

logging.basicConfig(
	level=logging.INFO,
	format="%(asctime)s [%(processName)s] %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

I = TypeVar("I")
O = TypeVar("O")


class IWorker(ABC, Generic[I, O]):
    def __init__(self, in_queue: GenMPQueue[I], out_queue: GenMPQueue[O]) -> None:
        self.in_queue: GenMPQueue[I] = in_queue
        self.out_queue: GenMPQueue[O] = out_queue

    @abstractmethod
    def work(self, item: I) -> O:
        pass


class IBatchProcessor(
    Generic[I, O],
    AbstractContextManager,
):

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
        n_workers: int,
        worker_generator: Callable[[GenMPQueue[I], GenMPQueue[O]], IWorker[I, O]],
        worker_timeout: float | None = None,
    ) -> None:
        self.in_queue: GenMPQueue[I] = in_queue
        self.out_queue: GenMPQueue[O] = out_queue
        self.n_workers = n_workers
        self.worker_generator = worker_generator
        self.stop_event: EventType = Event()
        self.workers: list[Process] = []
        self.worker_timeout = worker_timeout

    def _worker(self) -> None:
        worker = self.worker_generator(self.in_queue, self.out_queue)

        while not self.stop_event.is_set():
            try:
                item: I = self.in_queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                result: O = worker.work(item)
                self.out_queue.put(result)
            except Exception:
                logger.exception("Worker crashed while processing item")

    def start(self) -> None:
        self.stop_event.clear()
        for _ in range(self.n_workers):
            p = Process(target=self._worker)
            p.start()
            self.workers.append(p)

    def stop(self) -> None:
        self.stop_event.set()
        if not self.worker_timeout:
            for p in self.workers:
                p.join()
        else:
            for p in self.workers:
                p.join(timeout=self.worker_timeout)
            for p in self.workers:
                if p.is_alive():
                    p.terminate()
        self.workers.clear()

    def __enter__(self) -> "BatchProcessor":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stop()