from abc import ABC, abstractmethod
from typing import Generic, Iterable, TypeVar, List
import asyncio
from queue import Empty

from ..batch_processor.batch_processor import IBatchProcessor

I = TypeVar("I")
O = TypeVar("O")


class IIterableBatchProcessor(Generic[I, O], ABC):
    @abstractmethod
    async def process(self) -> Iterable[O]:
        pass


class IterableBatchProcessor(IIterableBatchProcessor[I, O]):
    def __init__(
        self,
        batch_processor: IBatchProcessor[I, O],
        in_iterable: Iterable[I],
        n_items: int,
    ):
        self._batch_processor: IBatchProcessor[I, O] = batch_processor
        self._in_iterable: Iterable[I] = in_iterable
        self._out_iterable: List[O] = []
        self._n_items: int = n_items
        self._items_queued: int = 0

    async def _queue_in_iterable(self):
        """Encola hasta n_items del iterable de entrada al batch processor."""
        in_iter = iter(self._in_iterable)
        for _ in range(self._n_items):
            try:
                item = next(in_iter)
                self._batch_processor.put(item)
                self._items_queued += 1
            except StopIteration:
                break  # Si no hay más elementos, parar

    async def _populate_out_iterable(self):
        """Saca elementos del batch processor y los mete al iterable de salida."""
        processed = 0
        while processed < self._items_queued:
            try:
                result = self._batch_processor.get_nowait()
                self._out_iterable.append(result)
                processed += 1
            except Empty:
                await asyncio.sleep(0.01)  # Esperar un poco antes de intentar de nuevo

    async def process(self) -> Iterable[O]:
        """Procesa los elementos usando el batch processor y retorna el iterable de salida."""
        with self._batch_processor:
            self._batch_processor.start()

            # Crear tareas para encolar y poblar concurrentemente
            queue_task = asyncio.create_task(self._queue_in_iterable())
            populate_task = asyncio.create_task(self._populate_out_iterable())

            # Ejecutar ambas tareas concurrentemente
            await asyncio.gather(queue_task, populate_task)

        return self._out_iterable
