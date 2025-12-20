from typing import Callable, Optional
from .worker_pool import IWorkerPool, WorkerPool
from .worker import IWorker


class WorkerPoolFactory:
    """
    Factory responsible for creating WorkerPool instances.

    This class encapsulates the construction logic of WorkerPool objects,
    allowing different configurations without exposing the creation details
    to the caller.
    """

    def create(
        self,
        n_workers: int,
        worker_factory: Callable[[], IWorker],
        worker_timeout: Optional[float] = None,
    ) -> IWorkerPool:
        """
        Create and configure a WorkerPool instance.

        Args:
                n_workers (int):
                        Number of worker instances to spawn in the pool.

                worker_factory (Callable[[], IWorker]):
                        A callable responsible for creating new worker instances.
                        It must return an object implementing the IWorker interface.

                worker_timeout (Optional[float], optional):
                        Maximum time in seconds a worker is allowed to run a task.
                        If None, workers will not have a timeout.
                        Defaults to None.

        Returns:
                WorkerPool:
                        A fully initialized WorkerPool instance configured with
                        the provided parameters.
        """
        return WorkerPool(
            n_workers=n_workers,
            worker_factory=worker_factory,
            worker_timeout=worker_timeout,
        )
