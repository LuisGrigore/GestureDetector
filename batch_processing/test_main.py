import asyncio
import time
from typing import List

# Importar las clases (ajusta las rutas según tu estructura)
from src.batch_processor.factory import BatchProcessorFactory
from src.batch_processor.batch_worker import IBatchWorker
from batch_processing.src.iterable_batch_processor.iterable_batch_processor import IterableBatchProcessor


class SimpleWorker(IBatchWorker[int, int]):
    def work(self, item: int) -> int:
        # Simular procesamiento
        time.sleep(0.1)
        return item * item


async def main():
    # Crear batch processor
    factory = BatchProcessorFactory()
    batch_proc = factory.create_with_default_settings(
        n_workers=2,
        worker_factory=SimpleWorker,
    )

    # Datos de entrada
    in_data = [1, 2, 3, 4, 5]

    # Crear processor iterable
    iterable_proc = IterableBatchProcessor(
        batch_processor=batch_proc,
        in_iterable=in_data,
        n_items=len(in_data),
    )

    # Procesar
    result = await iterable_proc.process()

    print("Resultados:", result)


if __name__ == "__main__":
    asyncio.run(main())
