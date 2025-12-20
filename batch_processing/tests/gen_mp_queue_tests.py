import pytest
import time
from queue import Empty, Full

from ..src.gen_mp_queue import GenMPQueue


# ---------------------------------------------------------------------------
# Comportamiento básico
# ---------------------------------------------------------------------------

def test_queue_starts_empty():
    q = GenMPQueue()
    assert q.empty()
    assert q.qsize() == 0
    assert not q.full()


def test_put_and_get_single_element():
    q = GenMPQueue[int]()
    q.put(42)
    result = q.get(timeout=0.1)

    assert result == 42
    assert q.empty()


def test_queue_fifo_order():
    q = GenMPQueue[int]()
    q.put(1)
    q.put(2)
    q.put(3)

    assert q.get(timeout=0.1) == 1
    assert q.get(timeout=0.1) == 2
    assert q.get(timeout=0.1) == 3


# ---------------------------------------------------------------------------
# nowait semantics (sin asumir disponibilidad inmediata)
# ---------------------------------------------------------------------------

def test_put_nowait_and_get_nowait_not_strict():
    q = GenMPQueue[str]()
    q.put_nowait("hello")

    time.sleep(0.01)

    assert q.get_nowait() == "hello"
    assert q.empty()


def test_get_nowait_empty_raises():
    q = GenMPQueue()
    with pytest.raises(Empty):
        q.get_nowait()


def test_put_nowait_full_raises():
    q = GenMPQueue(maxsize=1)
    q.put_nowait(1)
    with pytest.raises(Full):
        q.put_nowait(2)


# ---------------------------------------------------------------------------
# Timeouts
# ---------------------------------------------------------------------------

def test_get_timeout_raises_empty():
    q = GenMPQueue()
    start = time.time()

    with pytest.raises(Empty):
        q.get(timeout=0.05)

    assert time.time() - start >= 0.05


def test_put_timeout_raises_full():
    q = GenMPQueue(maxsize=1)
    q.put(1)

    start = time.time()
    with pytest.raises(Full):
        q.put(2, timeout=0.05)

    assert time.time() - start >= 0.05


# ---------------------------------------------------------------------------
# Estado: full, qsize
# ---------------------------------------------------------------------------

def test_full_and_qsize():
    q = GenMPQueue(maxsize=2)
    assert q.qsize() == 0
    assert not q.full()

    q.put(1)
    assert q.qsize() == 1

    q.put(2)
    assert q.qsize() == 2
    assert q.full()


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

def test_context_manager_closes_queue():
    q = GenMPQueue()

    with q as queue:
        queue.put(1)
        assert queue.get(timeout=0.1) == 1

    with pytest.raises((ValueError, OSError)):
        q.put(2)


# ---------------------------------------------------------------------------
# Close / join / cancel
# ---------------------------------------------------------------------------

def test_close_prevents_further_use():
    q = GenMPQueue()
    q.close()
    with pytest.raises((ValueError, OSError)):
        q.put(1)


def test_join_thread_and_cancel_join_thread_do_not_fail():
    q = GenMPQueue()
    q.put(1)
    q.close()

    q.cancel_join_thread()
    q.join_thread()
