from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from contextlib import AbstractContextManager
from exception_info import ExceptionInfo

I = TypeVar("I")
O = TypeVar("O")

class WorkerError(Exception):
    pass


class WorkerFatalError(WorkerError):
    def __init__(self, pid: int | None, exitcode: int | None):
        super().__init__(f"Worker pid={pid} died with exitcode={exitcode}")
        self.pid = pid
        self.exitcode = exitcode


class WorkerReportedError(WorkerError):
    def __init__(self, info: ExceptionInfo):
        super().__init__(
            f"Worker reported exception {info.exc_type}: {info.message}\n{info.tb}"
        )
        self.info = info


class IWorker(Generic[I, O], AbstractContextManager, ABC):
    @abstractmethod
    def work(self, item: I) -> O:
        pass

    def __enter__(self) -> IWorker[I, O]:
        return self

    def __exit__(self, exc_type, exc_value, tb) -> None:
        pass