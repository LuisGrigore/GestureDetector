class WorkerError(Exception):
    pass


class WorkerFatalError(WorkerError):
    def __init__(self, pid: int | None, exitcode: int | None):
        super().__init__(f"Worker pid={pid} died with exitcode={exitcode}")
        self.pid = pid
        self.exitcode = exitcode


class WorkerReportedError(WorkerError):
    def __init__(self, info):
        super().__init__(f"Worker reported exception: {info.message}")
        self.info = info