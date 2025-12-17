from multiprocessing import Event
from gen_mp_queue import GenMPQueue
from configuration import BatchProcessorConfig


class BatchContext:
    def __init__(self, config: BatchProcessorConfig):
        self.config = config
        self.in_queue = GenMPQueue()
        self.out_queue = GenMPQueue()
        self.error_queue = GenMPQueue()
        self.stop_event = Event()
        self.abort_event = Event()