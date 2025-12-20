from .monitor import IWorkerMonitor, WorkerMonitor
from .configuration import MonitorConfig
from .context import MonitorContext
from ..worker_pool.worker_pool import IWorkerPool, WorkerPool
from ..context import ControlContext
from ..configuration import SharedConfig, FailurePolicy


class MonitorFactory:
    """
    Factory class for creating WorkerMonitor instances with various configurations.

    This factory provides multiple methods to create monitors, allowing flexibility
    for different use cases, such as shared control contexts or standalone setups.
    """

    def create_with_shared_control_context(
        self,
        pool: IWorkerPool,
        monitor_config: MonitorConfig,
        control_ctx: ControlContext,
    ) -> IWorkerMonitor:
        """
        Create a WorkerMonitor using a shared ControlContext.

        This method is useful when the monitor needs to share control events
        (like stop/abort signals) with other components, such as a BatchProcessor.

        Args:
                pool (WorkerPool): The worker pool to monitor.
                monitor_config (MonitorConfig): Configuration for the monitor.
                control_ctx (ControlContext): Shared control context for events.

        Returns:
                WorkerMonitor: A configured monitor instance.
        """
        monitor_ctx = MonitorContext(monitor_config, control_ctx)
        return WorkerMonitor(pool, monitor_ctx)

    def create_independent_monitor(
        self,
        pool: WorkerPool,
        monitor_config: MonitorConfig,
    ) -> IWorkerMonitor:
        """
        Create an independent WorkerMonitor with its own ControlContext.

        This method creates a monitor that operates independently, with its own
        control events, suitable for standalone use without shared state.

        Args:
                pool (WorkerPool): The worker pool to monitor.
                monitor_config (MonitorConfig): Configuration for the monitor.

        Returns:
                WorkerMonitor: A configured monitor instance.
        """
        control_ctx = ControlContext()
        monitor_ctx = MonitorContext(monitor_config, control_ctx)
        return WorkerMonitor(pool, monitor_ctx)

    def create_with_default_settings(
        self,
        pool: WorkerPool,
        on_worker_death: FailurePolicy = FailurePolicy.RESTART,
        worker_monitoring_frequency: float = 1.0,
        logging: bool = True,
    ) -> IWorkerMonitor:
        """
        Create a WorkerMonitor with default settings.

        This convenience method creates a monitor using sensible defaults,
        allowing quick setup without specifying a full MonitorConfig.

        Args:
                pool (WorkerPool): The worker pool to monitor.
                on_worker_death (FailurePolicy): Policy for handling worker deaths. Defaults to RESTART.
                worker_monitoring_frequency (float): Frequency in seconds to check workers. Defaults to 1.0.
                logging (bool): Whether to enable logging. Defaults to True.

        Returns:
                WorkerMonitor: A monitor instance with default configurations.
        """
        shared_config = SharedConfig(logging=logging)
        monitor_config = MonitorConfig(
            shared=shared_config,
            on_worker_death=on_worker_death,
            worker_monitoring_frequency=worker_monitoring_frequency,
        )
        control_ctx = ControlContext()
        monitor_ctx = MonitorContext(monitor_config, control_ctx)
        return WorkerMonitor(pool, monitor_ctx)
