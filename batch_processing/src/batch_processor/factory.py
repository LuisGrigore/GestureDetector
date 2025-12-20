from typing import Callable, TypeVar, Optional
from .batch_processor import IBatchProcessor, BatchProcessor
from .batch_worker import BatchWorkerExecutor, IBatchWorker
from .context import BatchProcessorContext
from .configuration import BatchProcessorConfig, ProcessorConfig
from ..context import ControlContext
from ..monitor.factory import MonitorFactory
from ..monitor.monitor import IWorkerMonitor
from ..monitor.configuration import MonitorConfig
from ..worker_pool.factory import WorkerPoolFactory
from ..configuration import SharedConfig

I = TypeVar("I")
O = TypeVar("O")


class BatchProcessorFactory:
    """
    Factory class for creating BatchProcessor instances with various configurations.

    This factory provides multiple methods to create batch processors, supporting
    full creation from scratch or composition with existing components like pools
    and monitors for flexibility in different use cases.
    """

    def __init__(self):
        self.worker_pool_factory = WorkerPoolFactory()
        self.monitor_factory = MonitorFactory()

    def create(
        self,
        n_workers: int,
        worker_factory: Callable[[], IBatchWorker[I, O]],
        config: BatchProcessorConfig,
    ) -> IBatchProcessor[I, O]:
        """
        Create a complete BatchProcessor from scratch.

        This method builds all components internally, including the worker pool,
        monitor, and contexts, providing a full-featured batch processor.

        Args:
            n_workers (int): Number of worker processes.
            worker_factory (Callable[[], IBatchWorker[I, O]]): Factory for worker instances.
            config (BatchProcessorConfig): Configuration for the batch processor.

        Returns:
            IBatchProcessor[I, O]: A fully configured batch processor.
        """
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

        pool = self.worker_pool_factory.create(
            n_workers=n_workers,
            worker_factory=executor_factory,
            worker_timeout=config.worker_timeout,
        )

        monitor = self.monitor_factory.create_with_shared_control_context(
            pool, monitor_config, control_ctx
        )

        return BatchProcessor[I, O](pool, monitor, processor_ctx)

    def create_from_existing_monitor(
        self,
        monitor: IWorkerMonitor,
        n_workers: int,
        worker_factory: Callable[[], IBatchWorker[I, O]],
        config: BatchProcessorConfig,
    ) -> IBatchProcessor[I, O]:
        """
        Create a BatchProcessor using existing WorkerPool and WorkerMonitor.

        This method composes a batch processor from pre-existing pool and monitor,
        creating only the processor context, ideal for advanced customization.

        Args:
            pool (WorkerPool): Existing worker pool to use.
            monitor (WorkerMonitor): Existing monitor to use.
            worker_factory (Callable[[], IBatchWorker[I, O]]): Factory for worker instances.
            config (BatchProcessorConfig): Configuration for the batch processor.

        Returns:
            IBatchProcessor[I, O]: A batch processor using the provided components.
        """
        shared_config = SharedConfig(logging=config.logging)
        processor_config = ProcessorConfig(
            shared=shared_config, on_worker_exception=config.on_worker_exception
        )

        control_ctx = ControlContext()
        processor_ctx = BatchProcessorContext[I, O](processor_config, control_ctx)

        def executor_factory():
            return BatchWorkerExecutor[I, O](processor_ctx, worker_factory)

        pool = self.worker_pool_factory.create(
            n_workers=n_workers,
            worker_factory=executor_factory,
            worker_timeout=config.worker_timeout,
        )

        return BatchProcessor[I, O](pool, monitor, processor_ctx)

    def create_with_default_settings(
        self,
        n_workers: int,
        worker_factory: Callable[[], IBatchWorker[I, O]],
        on_worker_exception: str = "ABORT",
        on_worker_death: str = "RESTART",
        worker_monitoring_frequency: float = 1.0,
        logging: bool = True,
        worker_timeout: Optional[float] = None,
    ) -> IBatchProcessor[I, O]:
        """
        Create a BatchProcessor with default settings.

        This convenience method allows quick setup with sensible defaults,
        avoiding the need to specify a full BatchProcessorConfig.

        Args:
            n_workers (int): Number of worker processes.
            worker_factory (Callable[[], IBatchWorker[I, O]]): Factory for worker instances.
            on_worker_exception (str): Policy for worker exceptions ('IGNORE', 'ABORT'). Defaults to 'ABORT'.
            on_worker_death (str): Policy for worker deaths ('IGNORE', 'ABORT', 'RESTART'). Defaults to 'RESTART'.
            worker_monitoring_frequency (float): Monitoring frequency in seconds. Defaults to 1.0.
            logging (bool): Enable logging. Defaults to True.
            worker_timeout (float): Timeout for workers. Defaults to None.

        Returns:
            IBatchProcessor[I, O]: A batch processor with default configurations.
        """
        from .configuration import FailurePolicy

        config = BatchProcessorConfig(
            on_worker_exception=FailurePolicy[on_worker_exception],
            on_worker_death=FailurePolicy[on_worker_death],
            worker_monitoring_frequency=worker_monitoring_frequency,
            logging=logging,
            worker_timeout=worker_timeout,
        )
        return self.create(n_workers, worker_factory, config)
