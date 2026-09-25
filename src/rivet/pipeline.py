from __future__ import annotations

import asyncio

from rivet.frames import Frame
from rivet.processor import Processor
from rivet.runtime import ProcessorRuntime
from rivet.state import Generation


class Pipeline:
    """Wires processor runtimes into a chain and owns their ordering."""

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

    async def start(self) -> None:
        for runtime in self.runtimes:
            await runtime.start()

    async def push(self, frame: Frame) -> None:
        frame.generation = self.generation.value
        await self.runtimes[0].push(frame=frame)

    async def get_output(self) -> Frame:
        return await self.output_queue.get()

    async def interrupt(self) -> int:
        """Discard in-flight and queued work across every stage.

        Returns the number of stale frames dropped.
        """
        async with self._interrupt_lock:
            # Bump before sweeping: everything already in the system becomes
            # stale, and anything pushed from here survives.
            self.generation.bump()

            dropped = 0

            # Upstream first, so a stage is cleared before the one feeding it.
            for runtime in self.runtimes:
                dropped += await runtime.interrupt()

            return dropped + self._drop_stale_output()

    async def drain(self, timeout: float | None = None) -> bool:
        """Finish queued work in every stage, then stop.

        Returns False if any stage still held work when `timeout` elapsed.
        `timeout` is per stage, so worst case is timeout * len(runtimes).
        """
        async with self._shutdown_lock:
            drained = True

            # Upstream first, so a stage finishes feeding the next before that
            # one is drained.
            for runtime in self.runtimes:
                if not await runtime.drain(timeout=timeout):
                    drained = False

            return drained

    async def stop(self) -> None:
        async with self._shutdown_lock:
            for runtime in self.runtimes:
                await runtime.stop()

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

    def _drop_stale_output(self) -> int:
        kept: list[Frame] = []
        dropped = 0

        while not self.output_queue.empty():
            try:
                frame = self.output_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            if self.generation.is_stale(frame.generation):
                dropped += 1
            else:
                kept.append(frame)

        for frame in kept:
            self.output_queue.put_nowait(frame)

        return dropped
