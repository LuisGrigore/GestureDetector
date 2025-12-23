import asyncio
import time
from typing import List

# Importar las clases
from src.batch_processor.factory import BatchProcessorFactory
from src.batch_processor.batch_worker import IBatchWorker
from src.iterable_batch_processor.itergble_batch_processor import IterableBatchProcessor


class SimpleWorker(IBatchWorker[int, int]):
    def work(self, item: int) -> int:
        time.sleep(0.0001)  # Simular procesamiento
        return item * item


def simple_iteration(data: List[int]) -> List[int]:
    """Procesar datos iterando directamente."""
    result = []
    for item in data:
        time.sleep(0.0001)  # Misma simulación
        result.append(item * item)
    return result


async def benchmark():
    # Datos de prueba
    data = list(range(100000))  # 1000 elementos para benchmark
    print(f"Procesando {len(data)} elementos...")

    # Benchmark Batch Processor
    print("\n--- Benchmark Batch Processor ---")
    factory = BatchProcessorFactory()
    batch_proc = factory.create_with_default_settings(
        n_workers=8,
        worker_factory=SimpleWorker,
    )

    out_data: List[int] = []
    iterable_proc = IterableBatchProcessor(
        batch_processor=batch_proc,
        in_iterable=data,
        n_items=len(data),
    )

    start_time = time.perf_counter()
    result_batch = await iterable_proc.process()
    batch_time = time.perf_counter() - start_time
    print(f"Tiempo Batch Processor: {batch_time:.2f} segundos")

    # Benchmark Simple Iteration
    print("\n--- Benchmark Simple Iteration ---")
    start_time = time.perf_counter()
    result_simple = simple_iteration(data)
    simple_time = time.perf_counter() - start_time
    print(f"Tiempo Simple Iteration: {simple_time:.2f} segundos")

    # Comparación
    print("\n--- Comparación ---")
    print(f"Batch Processor es {simple_time / batch_time:.2f}x más rápido")
    print(
        f"Resultados iguales (ignorando orden): {sorted(result_batch) == sorted(result_simple)}"
    )


if __name__ == "__main__":
    asyncio.run(benchmark())
