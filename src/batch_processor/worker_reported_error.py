class WorkerReportedError(Exception):
    def __init__(self, info):
        super().__init__(f"Worker reported exception: {info.message}")
        self.info = info