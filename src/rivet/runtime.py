import asyncio
from enum import Enum

from rivet.frames import Frame
from rivet.processor import Processor


class RuntimeState(Enum):
    CREATED = "created"
    RUNNING = "running"
    DRAINING = "draining"
    STOPPED = "stopped"


class ProcessorRuntime:
    def __init__(
        self,
        processor: Processor,
        output_queue: asyncio.Queue[Frame] | None = None,
        max_queue_size: int = 100,
    ) -> None:
        self.processor = processor
        self.state = RuntimeState.CREATED

        self.input_queue: asyncio.Queue[Frame] = asyncio.Queue(maxsize=max_queue_size)

        self.output_queue: asyncio.Queue[Frame] = (
            output_queue
            if output_queue is not None
            else asyncio.Queue(maxsize=max_queue_size)
        )

        self.task: asyncio.Task | None = None

    async def start(self) -> None:
        if self.state != RuntimeState.CREATED:
            raise RuntimeError(f"Cannot start runtime from state {self.state}")

        self.state = RuntimeState.RUNNING
        self.task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            frame = await self.input_queue.get()

            try:
                output_frames = await self.processor.process(frame=frame)

                for output in output_frames:
                    await self.output_queue.put(output)

            finally:
                self.input_queue.task_done()

    async def push(self, frame: Frame) -> None:
        if self.state != RuntimeState.RUNNING:
            raise RuntimeError(f"Cannot push frame while runtime is {self.state}")

        await self.input_queue.put(frame)

    async def stop(self) -> None:
        if self.state == RuntimeState.STOPPED:
            return

        self.state = RuntimeState.STOPPED

        if self.task is not None:
            self.task.cancel()

            try:
                await self.task
            except asyncio.CancelledError:
                pass

    async def drain(self) -> None:
        if self.state != RuntimeState.RUNNING:
            return

        self.state = RuntimeState.DRAINING

        await self.input_queue.join()

        if self.task is not None:
            self.task.cancel()

            try:
                await self.task
            except asyncio.CancelledError:
                pass

        self.state = RuntimeState.STOPPED

    async def interrupt(self) -> None:
        if self.state != RuntimeState.RUNNING:
            return

        # Cancel the current worker
        if self.task is not None:
            self.task.cancel()

            try:
                await self.task
            except asyncio.CancelledError:
                pass

        # Remove frames waiting in the queue
        while not self.input_queue.empty():
            try:
                self.input_queue.get_nowait()
                self.input_queue.task_done()
            except asyncio.QueueEmpty:
                break

        self.task = asyncio.create_task(self._run())
