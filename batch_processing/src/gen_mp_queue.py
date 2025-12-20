from contextlib import AbstractContextManager
from multiprocessing import Queue
from typing import Generic, TypeVar, Optional

T = TypeVar("T")

class GenMPQueue(Generic[T], AbstractContextManager):
    def __init__(self, maxsize: int = 0):
        self._queue = Queue(maxsize)

    def put(self, obj: T, block: bool = True, timeout: Optional[float] = None) -> None:
        self._queue.put(obj, block, timeout)

    def get(self, block: bool = True, timeout: Optional[float] = None) -> T:
        return self._queue.get(block, timeout)

    def put_nowait(self, obj: T) -> None:
        self._queue.put_nowait(obj)

    def get_nowait(self) -> T:
        return self._queue.get_nowait()

    def empty(self) -> bool:
        return self._queue.empty()

    def full(self) -> bool:
        return self._queue.full()

    def qsize(self) -> int:
        return self._queue.qsize()

    def close(self) -> None:
        self._queue.close()

    def join_thread(self) -> None:
        self._queue.join_thread()

    def cancel_join_thread(self) -> None:
        self._queue.cancel_join_thread()

    def __enter__(self) -> "GenMPQueue":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._queue.close()