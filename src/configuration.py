from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class BatchProcessorConfig:
    n_workers: int
    worker_monitoring_frequency: float
    worker_timeout: Optional[float] = None
    stop_on_reported_exception: bool = False
    stop_on_worker_death: bool = True
    restart_dead_workers: bool = False
    logging: bool = True