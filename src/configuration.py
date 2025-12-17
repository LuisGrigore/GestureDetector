from __future__ import annotations

from dataclasses import dataclass

from enum import Enum, auto
from dataclasses import dataclass


class FailurePolicy(Enum):
    IGNORE = auto()
    ABORT = auto()
    RESTART = auto()


@dataclass
class BatchProcessorConfig:
    on_worker_exception: FailurePolicy
    on_worker_death: FailurePolicy
    worker_monitoring_frequency: float = 1.0
    logging: bool = True