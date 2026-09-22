import asyncio

from rivet.frames import Frame
from rivet.processor import Processor


class ProcessorRuntine:
    def __init__(
        self,
        processor: Processor,
        max_queue_size: int = 100,
    ) -> None:
        self.processor = processor
        self.queue: asyncio.Queue[Frame] = asyncio.Queue(maxsize=max_queue_size)

        self.task: asyncio.Task | None = None

    async def start(self) -> None:
        self.task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            frame = await self.queue.get()

            try:
                output_frames = await self.processor.process(frame=frame)

                for output in output_frames:
                    print(f"{self.processor.__class__.__name__}produced {output}")
            finally:
                self.queue.task_done()

    async def push(self, frame: Frame) -> None:
        await self.queue.put(frame)
