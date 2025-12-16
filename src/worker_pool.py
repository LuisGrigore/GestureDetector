from __future__ import annotations

from abc import ABC, abstractmethod
from multiprocessing import Process
from typing import Callable, Generic, Optional, List, TypeVar
from threading import Lock


class IWorker(ABC):
    @abstractmethod
    def target(self) -> None:
        pass


class IWorkerPool(ABC):
    @abstractmethod
    def start_workers(self) -> None:
        pass

    @abstractmethod
    def join_workers(self) -> None:
        pass

    @abstractmethod
    def cleanup_workers(self) -> None:
        pass

    @abstractmethod
    def restart_dead_workers(self) -> None:
        pass

    @abstractmethod
    def get_worker_list(self) -> List[Process]:
        pass


class WorkerPool(IWorkerPool):
    def __init__(
        self,
        n_workers: int,
        worker_factory: Callable[[], IWorker],
        worker_timeout: Optional[float],
        lock: Lock,
    ):
        self.n_workers = n_workers
        self.worker_factory: Optional[Callable[[], None]] = None
        self.worker_timeout = worker_timeout
        self.lock = lock
        self.workers: List[Process] = []
        
    def _worker_target(self) -> None:

    def start_workers(self, worker_target: Callable[[], None]) -> None:
        self.worker_target = worker_target
        with self.lock:
            for _ in range(self.n_workers):
                p = Process(target=self.worker_target)
                p.start()
                self.workers.append(p)

    def join_workers(self) -> None:
        self.worker_target = None
        with self.lock:
            for p in self.workers:
                if self.worker_timeout:
                    p.join(timeout=self.worker_timeout)
                else:
                    p.join()

    def cleanup_workers(self) -> None:
        with self.lock:
            for p in self.workers:
                if p.is_alive():
                    try:
                        p.terminate()
                    except Exception:
                        pass
            self.workers.clear()

    def restart_dead_workers(self) -> None:
        if not self.worker_target:
            raise RuntimeError("Workers have not been started.")
        alive = [p for p in self.workers if p.is_alive()]
        dead_count = self.n_workers - len(alive)
        if dead_count <= 0:
            return
        self.workers = alive
        for _ in range(dead_count):
            p = Process(target=self.worker_target)
            p.start()
            self.workers.append(p)

    def get_worker_list(self) -> List[Process]:
        return self.workers


def create_worker_pool(
    n_workers: int,
    worker_target: Callable[[], None],
    worker_timeout: Optional[float],
) -> WorkerPool:
    return WorkerPool(
        n_workers=n_workers,
        worker_target=worker_target,
        worker_timeout=worker_timeout,
        lock=Lock(),
    )
