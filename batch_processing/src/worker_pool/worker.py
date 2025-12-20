from abc import ABC, abstractmethod
from typing import TypeVar

I = TypeVar("I")
O = TypeVar("O")


class IWorker(ABC):
    @abstractmethod
    def target(self) -> None:
        pass
