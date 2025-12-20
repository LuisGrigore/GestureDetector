import time
import random
import os
from queue import Empty

from batch_processor.factory import BatchProcessorFactory
from batch_processor.batch_worker import IBatchWorker
from batch_processor.configuration import BatchProcessorConfig, FailurePolicy
from batch_processor.batch_processor import IBatchProcessor


# ----------------------------
# Worker de prueba
# ----------------------------
class TestWorker(IBatchWorker[int, int]):
    def work(self, item: int) -> int:
        # Simula trabajo
        time.sleep(0.2)

        # Caso 1: excepción normal
        if item == 5:
            raise ValueError("Error controlado en item 5")

        # Caso 2: muerte brutal del proceso
        if item == 9:
            os._exit(1)  # simula segfault / kill -9

        # Caso normal
        return item * item


# ----------------------------
# MAIN
# ----------------------------
def main():
    print("=== Iniciando BatchProcessor de prueba ===")

    config = BatchProcessorConfig(
        on_worker_exception=FailurePolicy.IGNORE,  # probar ABORT también
        on_worker_death=FailurePolicy.RESTART,  # probar ABORT también
        worker_monitoring_frequency=0.5,
        logging=True,
        worker_timeout=2.0,
    )

    factory = BatchProcessorFactory()
    processor: IBatchProcessor[int, int] = factory.create(
        n_workers=3,
        worker_factory=TestWorker,
        config=config,
    )

    try:
        with processor:
            processor.start()

            print("Insertando elementos poco a poco...\n")

            for i in range(15):
                print(f"[MAIN] Insertando item {i}")
                processor.put(i)

                # Inserción gradual
                time.sleep(random.uniform(0.1, 0.4))

                # Leer resultados parciales
                while True:
                    try:
                        result = processor.get_nowait()
                        print(f"[MAIN] Resultado recibido: {result}")
                    except Empty:
                        break

                # Leer excepciones reportadas
                for exc in processor.poll_exceptions():
                    print(
                        f"[MAIN] Excepción capturada: "
                        f"{exc.exc_type.__name__} -> {exc.message}"
                    )

            print("\nEsperando a que se procese la cola...")
            time.sleep(3)

            # Vaciar colas finales
            while True:
                try:
                    result = processor.get_nowait()
                    print(f"[MAIN] Resultado final: {result}")
                except Empty:
                    break

            for exc in processor.poll_exceptions():
                print(
                    f"[MAIN] Excepción final: "
                    f"{exc.exc_type.__name__} -> {exc.message}"
                )

    except Exception as exc:
        print("\n=== BatchProcessor abortó ===")
        print(f"Excepción propagada al main: {exc!r}")

    print("\n=== Fin del test ===")


if __name__ == "__main__":
    main()
