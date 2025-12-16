from __future__ import annotations

from abc import ABC, abstractmethod
from multiprocessing.synchronize import Event as EventType
from queue import Empty
from typing import Optional
import time
from threading import Thread
from gen_mp_queue import GenMPQueue
from worker_pool import IWorkerPool
from exception_info import ExceptionInfo
from configuration import BatchProcessorConfig
from worker import WorkerReportedError, WorkerFatalError
from logger import logger

class IWorkerMonitor(ABC):
	@abstractmethod
	def start_monitoring(self) -> None:
		pass
	@abstractmethod
	def stop_monitoring(self) -> None:
		pass

class WorkerMonitor(IWorkerMonitor):
	def __init__(
		self,
		worker_pool: IWorkerPool,
		error_queue: GenMPQueue[ExceptionInfo],
		stop_event: EventType,
		abort_event: Optional[EventType],
		config: BatchProcessorConfig,
		exception_to_raise: Optional[Exception],
	):
		self.worker_pool = worker_pool
		self.error_queue = error_queue
		self.stop_event = stop_event
		self.abort_event = abort_event
		self.config = config
		self.exception_to_raise = exception_to_raise
		self.monitor_thread: Optional[Thread] = None

	def start_monitoring(self) -> None:
		if (
			self.config.restart_dead_workers
			or self.config.stop_on_reported_exception
			or self.config.stop_on_worker_death
		):
			self.monitor_thread = Thread(target=self._monitor_loop, daemon=True)
			self.monitor_thread.start()

	def stop_monitoring(self) -> None:
		if self.monitor_thread:
			self.monitor_thread.join()

	def _monitor_loop(self) -> None:
		while not self.stop_event.is_set():
			try:
				self._monitor_once()
			except Exception as exc:
				if not self.exception_to_raise:
					self.exception_to_raise = exc
			time.sleep(self.config.worker_monitoring_frequency)

	def _monitor_once(self) -> None:
		if self.config.restart_dead_workers:
			self.worker_pool.restart_dead_workers()
		self._check_error_queue()
		self._check_fatal_workers()

	def _check_error_queue(self) -> None:
		while True:
			try:
				info = self.error_queue.get_nowait()
			except Empty:
				break
			if self.config.logging:
				logger.error(
					"Worker reported exception: %s: %s", info.exc_type, info.message
				)
			if self.config.stop_on_reported_exception:
				self.worker_pool.cleanup_workers()
				raise WorkerReportedError(info)
			else:
				self.error_queue.put(info)

	def _check_fatal_workers(self) -> None:
		for p in self.worker_pool.get_worker_list():
			if p.exitcode is not None and p.exitcode != 0:
				fatal = WorkerFatalError(p.pid, p.exitcode)
				if self.config.logging:
					logger.error(
						"Worker died unexpectedly: pid=%s exitcode=%s",
						fatal.pid,
						fatal.exitcode,
					)
				if self.config.stop_on_worker_death:
					if self.abort_event:
						self.abort_event.set()
					self.worker_pool.join_workers()
					self.worker_pool.cleanup_workers()
					raise fatal

