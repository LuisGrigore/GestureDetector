from typing import Callable, TypeVar
from .batch_processor import IBatchProcessor, BatchProcessor
from context import ControlContext
from .context import BatchProcessorContext
from monitor.configuration import MonitorConfig
from .batch_worker import BatchWorkerExecutor, IBatchWorker
from worker_pool.worker_pool import WorkerPool
from monitor.monitor import WorkerMonitor, MonitorContext
from .configuration import (
    BatchProcessorConfig,
    SharedConfig,
    ProcessorConfig,
)

I = TypeVar("I")
O = TypeVar("O")

def create_batch_processor(
    n_workers: int,
    worker_factory: Callable[[], IBatchWorker[I, O]],
    config: BatchProcessorConfig,
) -> IBatchProcessor[I, O]:

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

    pool = WorkerPool(
        n_workers=n_workers,
        worker_factory=executor_factory,
        worker_timeout=config.worker_timeout,
    )

    monitor_ctx = MonitorContext(monitor_config, control_ctx)

    monitor = WorkerMonitor[I, O](pool, monitor_ctx)

    return BatchProcessor[I, O](pool, monitor, processor_ctx)