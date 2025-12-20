from typing import Generic, TypeVar
from .exception_info import ExceptionInfo
from .configuration import ProcessorConfig
from ..context import ControlContext
from ..gen_mp_queue import GenMPQueue

I = TypeVar("I")
O = TypeVar("O")

class BatchProcessorContext(Generic[I, O]):
    def __init__(self, config: ProcessorConfig, control_ctx: ControlContext):
        self.config = config
        self.control_ctx = control_ctx
        self.in_queue = GenMPQueue[I]()
        self.out_queue = GenMPQueue[O]()
        self.error_queue = GenMPQueue[ExceptionInfo]()

    @property
    def stop_event(self):
        return self.control_ctx.stop_event

    @property
    def abort_event(self):
        return self.control_ctx.abort_event

    @property
    def fatal_exception(self):
        return self.control_ctx.fatal_exception

    @fatal_exception.setter
    def fatal_exception(self, value):
        self.control_ctx.fatal_exception = value
