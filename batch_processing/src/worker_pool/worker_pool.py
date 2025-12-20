from abc import ABC, abstractmethod
from multiprocessing import Process
from threading import Lock
from typing import Callable, Optional, List
from .worker import IWorker
from .worker_fatal_error import WorkerFatalError

class IWorkerPool(ABC):
	@abstractmethod
	def start(self) -> None:
		pass

	@abstractmethod
	def stop(self) -> None:
		pass

	@abstractmethod
	def cleanup(self) -> None:
		pass

	@abstractmethod
	def restart_dead(self) -> int:
		pass

	@abstractmethod
	def fatal_errors(self) -> List[WorkerFatalError]:
		pass


class WorkerPool(IWorkerPool):
	def __init__(
		self,
		n_workers: int,
		worker_factory: Callable[[], IWorker],
		worker_timeout: Optional[float],
	):
		self._n_workers = n_workers
		self._worker_factory = worker_factory
		self._timeout = worker_timeout
		self._workers: List[Process] = []
		self._lock = Lock()
		self._started = False

	def _spawn(self) -> Process:
		worker = self._worker_factory()
		p = Process(target=worker.target)
		p.start()
		return p

	def start(self) -> None:
		with self._lock:
			if self._started:
				raise RuntimeError("WorkerPool already started")
			self._workers = [self._spawn() for _ in range(self._n_workers)]
			self._started = True

	def stop(self) -> None:
		with self._lock:
			for p in self._workers:
				p.join(timeout=self._timeout)

	def cleanup(self) -> None:
		with self._lock:
			for p in self._workers:
				if p.is_alive():
					p.terminate()
			self._workers.clear()
			self._started = False

	def restart_dead(self) -> int:
		with self._lock:
			alive = [p for p in self._workers if p.is_alive()]
			dead = self._n_workers - len(alive)
			self._workers = alive
			for _ in range(dead):
				self._workers.append(self._spawn())
			return dead

	def fatal_errors(self) -> List[WorkerFatalError]:
		return [
			WorkerFatalError(p.pid, p.exitcode)
			for p in self._workers
			if p.exitcode not in (None, 0)
		]
