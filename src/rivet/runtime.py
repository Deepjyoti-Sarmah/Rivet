import asyncio

from rivet.frames import Frame
from rivet.processor import Processor


class ProcessorRuntine:
    def __init__(
        self,
        processor: Processor,
        output_queue: asyncio.Queue[Frame] | None = None,
        max_queue_size: int = 100,
    ) -> None:
        self.processor = processor

        self.input_queue: asyncio.Queue[Frame] = asyncio.Queue(maxsize=max_queue_size)

        self.output_queue = output_queue

        self.task: asyncio.Task | None = None

    async def start(self) -> None:
        self.task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            frame = await self.input_queue.get()

            try:
                output_frames = await self.processor.process(frame=frame)

                if self.output_queue is not None:
                    for output in output_frames:
                        await self.output_queue.put(output)
            finally:
                self.input_queue.task_done()

    async def push(self, frame: Frame) -> None:
        await self.input_queue.put(frame)

    async def stop(self) -> None:
        if self.task is not None:
            self.task.cancel()

            try:
                await self.task
            except asyncio.CancelledError:
                pass
