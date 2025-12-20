from __future__ import annotations

from dataclasses import dataclass
from ..configuration import FailurePolicy, SharedConfig

@dataclass
class MonitorConfig:
    shared: SharedConfig
    on_worker_death: FailurePolicy
    worker_monitoring_frequency: float = 1.0