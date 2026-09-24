from __future__ import annotations

import asyncio

from rivet.frames import Frame
from rivet.processor import Processor
from rivet.runtime import Generation, ProcessorRuntime


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

        self.generation = Generation()
        self._interrupt_lock = asyncio.Lock()
        self._shutdown_lock = asyncio.Lock()

        self._build()

    def _build(self) -> None:
        next_queue = self.output_queue

        for processor in reversed(self.processors):
            runtime = ProcessorRuntime(
                processor=processor,
                output_queue=next_queue,
                max_queue_size=self.max_queue_size,
                generation=self.generation,
            )

            self.runtimes.insert(0, runtime)
            next_queue = runtime.input_queue

    async def start(self) -> None:
        for runtime in self.runtimes:
            await runtime.start()

    async def push(self, frame: Frame) -> None:
        frame.generation = self.generation.value
        await self.runtimes[0].push(frame=frame)

    async def get_output(self) -> Frame:
        return await self.output_queue.get()

    async def interrupt(self) -> int:
        async with self._interrupt_lock:
            # Bump first: every frame already in the system is now stale by
            # definition, and anything pushed from here carries the new number
            # and survives the sweep.
            self.generation.bump()

            dropped = 0

            for runtime in self.runtimes:
                dropped += await runtime.interrupt()

            dropped += self._flush_output()

            return dropped

    def _flush_output(self) -> int:
        kept: list[Frame] = []
        dropped = 0

        while not self.output_queue.empty():
            try:
                frame = self.output_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            if frame.generation < self.generation.value:
                dropped += 1
            else:
                kept.append(frame)

        for frame in kept:
            self.output_queue.put_nowait(frame)

        return dropped

    async def drain(self, timeout: float | None = None) -> bool:
        """Finish queued work in every stage, then stop.

        Swept source -> sink so each stage has finished feeding the next before
        that one is drained. Returns True only if every stage emptied; a False
        anywhere means work was dropped.

        `timeout` is per stage, so worst-case total is timeout * len(runtimes).
        """
        async with self._shutdown_lock:
            drained = True

            for runtime in self.runtimes:
                if not await runtime.drain(timeout=timeout):
                    drained = False

            return drained

    async def stop(self) -> None:
        async with self._shutdown_lock:
            for runtime in self.runtimes:
                await runtime.stop()
