from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from ..configuration import FailurePolicy, SharedConfig


@dataclass
class ProcessorConfig:
    shared: SharedConfig
    on_worker_exception: FailurePolicy



@dataclass
class BatchProcessorConfig:
    on_worker_exception: FailurePolicy
    on_worker_death: FailurePolicy
    worker_monitoring_frequency: float = 1.0
    logging: bool = True
    worker_timeout: Optional[float] = None
