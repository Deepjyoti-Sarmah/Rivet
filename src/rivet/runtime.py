import asyncio

from rivet.frames import Frame
from rivet.processor import Processor


class ProcessorRuntime:
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
                import asyncio

                import pytest

                from rivet.frames import TextFrame
                from rivet.processors.exclamation import ExclamationProcessor
                from rivet.processors.uppercase import UppercaseProcessor
                from rivet.runtime import ProcessorRuntime

                @pytest.mark.asyncio
                async def test_two_processor_pipeline():
                    queue_2: asyncio.Queue[TextFrame] = asyncio.Queue()
                    queue_3: asyncio.Queue[TextFrame] = asyncio.Queue()

                    uppercase = ProcessorRuntime(
                        UppercaseProcessor(),
                        output_queue=queue_2,
                    )

                    exclamation = ProcessorRuntime(
                        ExclamationProcessor(),
                        output_queue=queue_3,
                    )

                    await uppercase.start()
                    await exclamation.start()

                    async def forward() -> None:
                        while True:
                            frame = await queue_2.get()

                            try:
                                await exclamation.push(frame)
                            finally:
                                queue_2.task_done()

                    forward_task = asyncio.create_task(forward())

                    await uppercase.push(TextFrame("hello"))

                    await uppercase.input_queue.join()
                    await queue_2.join()
                    await exclamation.input_queue.join()

                    result = await queue_3.get()

                    try:
                        assert result.text == "HELLO!"
                    finally:
                        queue_3.task_done()

                    forward_task.cancel()

                    try:
                        await forward_task
                    except asyncio.CancelledError:
                        pass

                    await uppercase.stop()
                    await exclamation.stop()
