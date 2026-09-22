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
        self.forward_tasks: list[asyncio.Task] = []

        self.output_queue: asyncio.Queue[Frame] = asyncio.Queue()

        self._build()

    def _build(self) -> None:
        for i, processor in enumerate(self.processors):
            if i == len(self.processors) - 1:
                output_queue = self.output_queue
            else:
                output_queue = asyncio.Queue(maxsize=self.max_queue_size)

            runtime = ProcessorRuntime(
                processor=processor,
                output_queue=output_queue,
                max_queue_size=self.max_queue_size,
            )

            self.runtimes.append(runtime)

    async def start(self) -> None:
        for runtime in self.runtimes:
            await runtime.start()

        for i in range(len(self.runtimes) - 1):
            current = self.runtimes[i]
            next_runtime = self.runtimes[i + 1]

            task = asyncio.create_task(
                self._forward(
                    current.output_queue,
                    next_runtime,
                )
            )

            self.forward_tasks.append(task)

    async def _forward(
        self,
        queue: asyncio.Queue[Frame],
        next_runtime: ProcessorRuntime,
    ) -> None:
        while True:
            frame = await queue.get()

            try:
                await next_runtime.push(frame=frame)
            finally:
                queue.task_done()

    async def push(self, frame: Frame) -> None:
        await self.runtimes[0].push(frame=frame)

    async def get_output(self) -> Frame:
        return await self.output_queue.get()

    async def stop(self) -> None:
        for task in self.forward_tasks:
            task.cancel()

        for task in self.forward_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

        for runtime in self.runtimes:
            await runtime.stop()
