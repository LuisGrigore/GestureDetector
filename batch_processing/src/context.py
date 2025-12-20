from multiprocessing import Event
from typing import Optional


class ControlContext:
    def __init__(self):
        self.stop_event = Event()
        self.abort_event = Event()
        self.fatal_exception: Optional[Exception] = None
