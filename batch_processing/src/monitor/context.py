from .configuration import MonitorConfig
from ..context import ControlContext

class MonitorContext:
    def __init__(self, config: MonitorConfig, control_ctx: ControlContext):
        self.config = config
        self.control_ctx = control_ctx

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