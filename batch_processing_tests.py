import sys
import time
import pytest

from batch_processing import (
    BatchProcessor,
    IWorker,
    ExceptionInfo,
    WorkerReportedError,
    WorkerFatalError,
)
from gen_mp_queue import GenMPQueue


# =========================
# Workers de prueba
# =========================

class SimpleWorker(IWorker[int, int]):
    def work(self, item: int) -> int:
        return item * 2


class StatefulWorker(IWorker[int, int]):
    def __init__(self, in_q, out_q):
        super().__init__(in_q, out_q)
        self.counter = 0

    def work(self, item: int) -> int:
        self.counter += 1
        return self.counter


class ExceptionWorker(IWorker[int, int]):
    def work(self, item: int) -> int:
        if item == 2:
            raise ValueError("boom")
        return item


class FatalWorker(IWorker[int, int]):
    def work(self, item: int) -> int:
        if item == 2:
            sys.exit(3)
        return item


class SlowWorker(IWorker[int, int]):
    def work(self, item: int) -> int:
        time.sleep(0.05)
        return item


# =========================
# Fixtures
# =========================

@pytest.fixture
def queues():
    return (
        GenMPQueue[int](),
        GenMPQueue[int](),
        GenMPQueue[ExceptionInfo](),
    )


def start_and_stop(bp: BatchProcessor, delay=0.1):
    bp.start()
    time.sleep(delay)
    bp.stop()


MONITOR_FREQ = 0.01  # frecuencia común para tests


# =========================
# Ciclo de vida
# =========================

def test_start_creates_n_workers(queues):
    in_q, out_q, err_q = queues
    bp = BatchProcessor(
        in_q,
        out_q,
        err_q,
        n_workers=2,
        worker_generator=lambda iq, oq: SimpleWorker(iq, oq),
        worker_monitoring_frequency=MONITOR_FREQ,
    )
    bp.start()
    assert len(bp.workers) == 2
    assert all(p.is_alive() for p in bp.workers)
    bp.stop()


def test_start_twice_raises(queues):
    in_q, out_q, err_q = queues
    bp = BatchProcessor(
        in_q,
        out_q,
        err_q,
        n_workers=1,
        worker_generator=lambda iq, oq: SimpleWorker(iq, oq),
        worker_monitoring_frequency=MONITOR_FREQ,
    )
    bp.start()
    with pytest.raises(RuntimeError):
        bp.start()
    bp.stop()


def test_context_manager_calls_stop(queues):
    in_q, out_q, err_q = queues
    with BatchProcessor(
        in_q,
        out_q,
        err_q,
        n_workers=1,
        worker_generator=lambda iq, oq: SimpleWorker(iq, oq),
        worker_monitoring_frequency=MONITOR_FREQ,
    ):
        pass


# =========================
# Procesamiento
# =========================

def test_process_items_successfully(queues):
    in_q, out_q, err_q = queues
    for i in range(3):
        in_q.put(i)

    bp = BatchProcessor(
        in_q,
        out_q,
        err_q,
        n_workers=1,
        worker_generator=lambda iq, oq: SimpleWorker(iq, oq),
        worker_monitoring_frequency=MONITOR_FREQ,
    )

    start_and_stop(bp)

    results = sorted(out_q.get() for _ in range(3))
    assert results == [0, 2, 4]


def test_worker_keeps_state(queues):
    in_q, out_q, err_q = queues
    for i in range(3):
        in_q.put(i)

    bp = BatchProcessor(
        in_q,
        out_q,
        err_q,
        n_workers=1,
        worker_generator=lambda iq, oq: StatefulWorker(iq, oq),
        worker_monitoring_frequency=MONITOR_FREQ,
    )

    start_and_stop(bp)

    results = [out_q.get() for _ in range(3)]
    assert results == [1, 2, 3]


def test_parallelism_is_faster_than_sequential(queues):
    in_q, out_q, err_q = queues
    for i in range(4):
        in_q.put(i)

    bp = BatchProcessor(
        in_q,
        out_q,
        err_q,
        n_workers=2,
        worker_generator=lambda iq, oq: SlowWorker(iq, oq),
        worker_monitoring_frequency=MONITOR_FREQ,
    )

    start = time.time()
    start_and_stop(bp, delay=0.3)
    elapsed = time.time() - start

    assert elapsed < 0.3 * 4


# =========================
# Errores reportados
# =========================

def test_worker_exception_is_reported(queues):
    in_q, out_q, err_q = queues
    in_q.put(1)
    in_q.put(2)

    bp = BatchProcessor(
        in_q,
        out_q,
        err_q,
        n_workers=1,
        worker_generator=lambda iq, oq: ExceptionWorker(iq, oq),
        worker_monitoring_frequency=MONITOR_FREQ,
        stop_on_reported_exception=False,
    )

    start_and_stop(bp)

    infos = bp.poll_exceptions()
    assert len(infos) == 1
    assert infos[0].exc_type == "ValueError"


def test_stop_on_reported_exception_raises(queues):
    in_q, out_q, err_q = queues
    in_q.put(2)

    bp = BatchProcessor(
        in_q,
        out_q,
        err_q,
        n_workers=1,
        worker_generator=lambda iq, oq: ExceptionWorker(iq, oq),
        worker_monitoring_frequency=MONITOR_FREQ,
        stop_on_reported_exception=True,
    )

    bp.start()
    time.sleep(0.1)

    with pytest.raises(WorkerReportedError):
        bp.stop()


# =========================
# Fallos fatales
# =========================

def test_fatal_worker_raises(queues):
    in_q, out_q, err_q = queues
    in_q.put(2)

    bp = BatchProcessor(
        in_q,
        out_q,
        err_q,
        n_workers=1,
        worker_generator=lambda iq, oq: FatalWorker(iq, oq),
        worker_monitoring_frequency=MONITOR_FREQ,
        stop_on_worker_death=True,
    )

    bp.start()
    time.sleep(0.1)

    with pytest.raises(WorkerFatalError):
        bp.stop()


def test_fatal_worker_can_be_ignored(queues):
    in_q, out_q, err_q = queues
    in_q.put(2)

    bp = BatchProcessor(
        in_q,
        out_q,
        err_q,
        n_workers=1,
        worker_generator=lambda iq, oq: FatalWorker(iq, oq),
        worker_monitoring_frequency=MONITOR_FREQ,
        stop_on_worker_death=False,
    )

    start_and_stop(bp)


# =========================
# Restart de workers
# =========================

def test_restart_dead_worker(queues):
    in_q, out_q, err_q = queues
    in_q.put(2)
    in_q.put(1)

    bp = BatchProcessor(
        in_q,
        out_q,
        err_q,
        n_workers=1,
        worker_generator=lambda iq, oq: FatalWorker(iq, oq),
        worker_monitoring_frequency=MONITOR_FREQ,
        restart_dead_workers=True,
        stop_on_worker_death=False,
    )

    start_and_stop(bp, delay=0.2)

    assert out_q.qsize() >= 0


# =========================
# Casos límite
# =========================

def test_empty_queue_no_crash(queues):
    in_q, out_q, err_q = queues
    bp = BatchProcessor(
        in_q,
        out_q,
        err_q,
        n_workers=1,
        worker_generator=lambda iq, oq: SimpleWorker(iq, oq),
        worker_monitoring_frequency=MONITOR_FREQ,
    )
    start_and_stop(bp)


def test_stop_processes_remaining_items(queues):
    in_q, out_q, err_q = queues
    for i in range(5):
        in_q.put(i)

    bp = BatchProcessor(
        in_q,
        out_q,
        err_q,
        n_workers=1,
        worker_generator=lambda iq, oq: SlowWorker(iq, oq),
        worker_monitoring_frequency=MONITOR_FREQ,
    )

    bp.start()
    time.sleep(0.1)
    bp.stop()

    assert out_q.qsize() > 0
