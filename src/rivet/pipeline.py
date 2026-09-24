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

        self._interrupt_lock = asyncio.Lock()

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

    async def interrupt(self) -> int:
        """Discard in-flight and queued work across every stage.

        Swept source -> sink: clearing an upstream stage first stops it emitting
        fresh stale frames into stages already cleared.
        """
        async with self._interrupt_lock:
            dropped = 0

            for runtime in self.runtimes:
                dropped += await runtime.interrupt()

            dropped += self._flush_output()

            return dropped

    def _flush_output(self) -> int:
        dropped = 0

        while not self.output_queue.empty():
            try:
                self.output_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            dropped += 1

        return dropped

    async def stop(self) -> None:
        for runtime in self.runtimes:
            await runtime.stop()
