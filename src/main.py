import sys
import time
from gen_mp_queue import GenMPQueue
from batch_processing import BatchProcessor, BatchProcessorConfig
from worker import IWorker, WorkerReportedError, WorkerFatalError
from exception_info import ExceptionInfo

# Suponiendo que IWorker y BatchProcessor ya están definidos


class ExampleWorker(IWorker[int, str]):
    def work(self, item: int) -> str:
        if item == 3:
            raise ValueError("Boom triggered")
        if item == 5:
            sys.exit(2)  # simulación de salida fatal
        return f"Processed {item}"


if __name__ == "__main__":
    in_q: GenMPQueue[int] = GenMPQueue()
    out_q: GenMPQueue[str] = GenMPQueue()
    err_q: GenMPQueue[ExceptionInfo] = GenMPQueue()

    # poner algunos items en la cola
    for i in range(1, 7):
        in_q.put(i)

    config = BatchProcessorConfig(
        n_workers=2,
        worker_monitoring_frequency=0.1,
        stop_on_reported_exception=False,
        stop_on_worker_death=False,
        restart_dead_workers=True,
    )

    bp = BatchProcessor(
        in_queue=in_q,
        out_queue=out_q,
        error_queue=err_q,
        worker_generator=lambda : ExampleWorker(),
        config=config,
    )

    bp.start()

    # dar tiempo a procesar
    time.sleep(0.1)

    try:
        bp.stop()
    except WorkerReportedError as wre:
        print("Parent caught reported error:", wre)
    except WorkerFatalError as wfe:
        print("Parent caught fatal worker death:", wfe)

    # mostrar resultados procesados
    while not out_q.empty():
        print("Result:", out_q.get())

    # mostrar errores reportados
    while not err_q.empty():
        info = err_q.get()
        print("Reported error:", info)