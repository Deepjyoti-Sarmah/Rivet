from __future__ import annotations

import asyncio

from rivet.frames import Frame
from rivet.processor import Processor
from rivet.runtime import ProcessorRuntime


class Pipeline:
    def __init__(
        self,
        processors: list[Processor],
        max_queue_size: int = 100,
    ) -> None:
        self.processors = processors
        self.max_queue_size = max_queue_size

        self.runtimes: list[ProcessorRuntime] = []

        self.output_queue: asyncio.Queue[Frame] = asyncio.Queue(maxsize=max_queue_size)

        self._build()

    def _build(self) -> None:
        next_queue = self.output_queue

        for processor in reversed(self.processors):
            runtime = ProcessorRuntime(
                processor=processor,
                output_queue=next_queue,
                max_queue_size=self.max_queue_size,
            )

            self.runtimes.insert(0, runtime)
            next_queue = runtime.input_queue

    async def start(self) -> None:
        for runtime in self.runtimes:
            await runtime.start()

    async def push(self, frame: Frame) -> None:
        await self.runtimes[0].push(frame=frame)

    async def get_output(self) -> Frame:
        return await self.output_queue.get()

    async def stop(self) -> None:
        for runtime in self.runtimes:
            await runtime.stop()
