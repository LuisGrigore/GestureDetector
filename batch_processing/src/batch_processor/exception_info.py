import traceback
from dataclasses import dataclass
from typing import Any, Type


@dataclass
class ExceptionInfo:
    exc_type: Type[BaseException]
    message: str
    tb: str
    item: Any

    @classmethod
    def from_exception(cls, exc: Exception, item: Any) -> "ExceptionInfo":
        return cls(
            exc_type=type(exc),
            message=str(exc),
            tb=traceback.format_exc(),
            item=item,
        )