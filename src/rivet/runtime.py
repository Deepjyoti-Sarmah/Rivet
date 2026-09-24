import asyncio
from enum import Enum

from rivet.frames import Frame
from rivet.processor import Processor


class RuntimeState(Enum):
    CREATED = "created"
    RUNNING = "running"
    DRAINING = "draining"
    STOPPED = "stopped"
    FAILED = "failed"


class Generation:
    """The current turn number, shared by every stage in a pipeline"""

    def __init__(self) -> None:
        self.value = 0

    def bump(self) -> int:
        self.value += 1
        return self.value


class ProcessorRuntime:
    def __init__(
        self,
        processor: Processor,
        output_queue: asyncio.Queue[Frame] | None = None,
        max_queue_size: int = 100,
        generation: Generation | None = None,
    ) -> None:
        self.processor: Processor = processor

        self._own_generation = generation is None
        self.generation = generation if generation is not None else Generation()

        self.state = RuntimeState.CREATED
        self.error: Exception | None = None

        self.input_queue: asyncio.Queue[Frame] = asyncio.Queue(maxsize=max_queue_size)

        self.output_queue: asyncio.Queue[Frame] = (
            output_queue
            if output_queue is not None
            else asyncio.Queue(maxsize=max_queue_size)
        )

        self.task: asyncio.Task | None = None

        # Serialises stop/drain/interrupt so they cannot interleave across the
        # await points inside each other
        self._lifecycle_lock = asyncio.Lock()

    async def start(self) -> None:
        if self.state != RuntimeState.CREATED:
            raise RuntimeError(f"Cannot start runtime from state {self.state}")

        self.state = RuntimeState.RUNNING
        self.task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            frame = await self.input_queue.get()

            try:
                if frame.generation < self.generation.value:
                    # Belongs to a turn that has been superseded
                    continue

                output_frames = await self.processor.process(frame=frame)

                for output in output_frames:
                    # Processors build fresh frames and know nothing about
                    # turns, so thee runtime carries the stamp across

                    output.generation = frame.generation
                    await self.output_queue.put(output)

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                # Without this the task dies silently, the runtime keeps
                # reporting RUNNING, and push() accepts frames nobody processes
                self.error = exc
                self.state = RuntimeState.FAILED
                return

            finally:
                self.input_queue.task_done()

    async def push(self, frame: Frame) -> None:
        if self.state != RuntimeState.RUNNING:
            raise RuntimeError(f"Cannot push frame while runtime is {self.state}")

        await self.input_queue.put(frame)

    async def _cancel_worker(self) -> None:
        """Cancel the worker and wait for it to actually finish unwinding."""

        if self.task is None:
            return

        self.task.cancel()

        try:
            await self.task
        except asyncio.CancelledError:
            # We requested this cancellation, so it is ours to absorb
            pass

        self.task = None

    def _flush_input(self) -> int:
        """Discard queue frames. Returns how many were dropped."""
        kept: list[Frame] = []
        dropped = 0

        while not self.input_queue.empty():
            try:
                frame = self.input_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            self.input_queue.task_done()

            if frame.generation < self.generation.value:
                dropped += 1
            else:
                kept.append(frame)

        for frame in kept:
            self.input_queue.put_nowait(frame)

        return dropped

    async def stop(self) -> None:
        async with self._lifecycle_lock:
            if self.state in (RuntimeState.STOPPED, RuntimeState.FAILED):
                return

            self.state = RuntimeState.STOPPED
            await self._cancel_worker()

    async def drain(self) -> None:
        async with self._lifecycle_lock:
            if self.state != RuntimeState.RUNNING:
                return

            self.state = RuntimeState.DRAINING

        # Released while waiting so a concurrent stop() can still get in
        await self.input_queue.join()

        async with self._lifecycle_lock:
            if self.state != RuntimeState.DRAINING:
                return

            await self._cancel_worker()
            self.state = RuntimeState.STOPPED

    async def interrupt(self) -> int:
        async with self._lifecycle_lock:
            if self.state != RuntimeState.RUNNING:
                return 0
                # Only when standalone. In a pipeline the counter is shared and
                # Pipeline.interrupt() has already bumped it; bumping again here
                # would advance it once per stage.

            if self._own_generation:
                self.generation.bump()

            await self._cancel_worker()
            dropped = self._flush_input()
            self.task = asyncio.create_task(self._run())

            return dropped
