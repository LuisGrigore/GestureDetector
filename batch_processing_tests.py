from batch_processing import (
    BatchProcessorFactory,
    BatchProcessorConfig,
    IBatchProcessor,
    IBatchWorker,
    FailurePolicy,
)

class BatchWorker(IBatchWorker[int, int]):
    def __init__(self) -> None:
        super().__init__()

    def work(self, item: int) -> int:
        return item * 2


def worker_factory() -> IBatchWorker:
    return BatchWorker()


factory = BatchProcessorFactory()


if __name__ == "__main__":
    with factory.create(
        n_workers=20,
        worker_factory=worker_factory,
        config=BatchProcessorConfig(
            on_worker_death=FailurePolicy.ABORT, on_worker_exception=FailurePolicy.ABORT
        ),
    ) as processor:
        processor: IBatchProcessor[int,int] = processor
        processor.put(10)
        processor.put(20)
        processor.put(30)
        print(processor.get())
        print(processor.get())
        print(processor.get())
