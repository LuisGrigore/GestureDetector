from __future__ import annotations

import traceback
from dataclasses import dataclass
from typing import Generic, TypeVar

I = TypeVar("I")

@dataclass
class ExceptionInfo(Generic[I]):
    exc_type: str
    message: str
    tb: str
    item_repr: str

    @classmethod
    def from_exception(cls, exc: Exception, item: I) -> "ExceptionInfo":
        return cls(
            exc_type=type(exc).__name__,
            message=str(exc),
            tb=traceback.format_exc(),
            item_repr=repr(item),
        )