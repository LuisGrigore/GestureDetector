from __future__ import annotations

from dataclasses import dataclass

from enum import Enum, auto
from dataclasses import dataclass


class FailurePolicy(Enum):
    IGNORE = auto()
    ABORT = auto()
    RESTART = auto()


@dataclass
class SharedConfig:
    logging: bool = True