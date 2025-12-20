class WorkerFatalError(Exception):
    def __init__(self, pid: int | None, exitcode: int | None):
        super().__init__(f"Worker pid={pid} died with exitcode={exitcode}")
        self.pid = pid
        self.exitcode = exitcode
