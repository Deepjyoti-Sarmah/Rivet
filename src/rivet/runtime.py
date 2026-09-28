import asyncio

from rivet.frames import Frame
from rivet.processor import Processor
from rivet.state import Generation, RuntimeState

TERMINAL_STATES = (RuntimeState.STOPPED, RuntimeState.FAILED)


class ProcessorRuntime:
    """Runs one processor continuously: queues, worker task, lifecycle."""

    def __init__(
        self,
        processor: Processor,
        output_queue: asyncio.Queue[Frame] | None = None,
        max_queue_size: int = 100,
        generation: Generation | None = None,
    ) -> None:
        self.processor = processor

        self._owns_generation = generation is None
        self.generation = generation or Generation()

        self.state = RuntimeState.CREATED
        self.error: Exception | None = None

        self.input_queue: asyncio.Queue[Frame] = asyncio.Queue(maxsize=max_queue_size)
        self.output_queue = output_queue or asyncio.Queue(maxsize=max_queue_size)

        self.task: asyncio.Task | None = None
        self._inflight: Frame | None = None
        self._lifecycle_lock = asyncio.Lock()

    async def start(self) -> None:
        if self.state != RuntimeState.CREATED:
            raise RuntimeError(f"Cannot start runtime from state {self.state}")

        self.state = RuntimeState.RUNNING
        self.task = asyncio.create_task(self._run())

    async def push(self, frame: Frame) -> None:
        if self.state != RuntimeState.RUNNING:
            raise RuntimeError(f"Cannot push frame while runtime is {self.state}")

        await self.input_queue.put(frame)

    async def stop(self) -> None:
        async with self._lifecycle_lock:
            if self.state in TERMINAL_STATES:
                return

            await self._shutdown()

    async def drain(self, timeout: float | None = None) -> bool:
        """Finish queued work, then stop.

        Returns False if `timeout` elapsed with work still outstanding.
        """
        async with self._lifecycle_lock:
            if self.state != RuntimeState.RUNNING:
                return False

            self.state = RuntimeState.DRAINING

        drained = await self._wait_for_empty(timeout)

        # Lock was released across the wait, so a concurrent stop() may own the
        # shutdown by now.
        async with self._lifecycle_lock:
            if self.state == RuntimeState.DRAINING:
                await self._shutdown()

        return drained

    async def interrupt(self) -> int:
        """Discard stale work, keep the runtime alive.

        A worker holding a current frame is left running, so `interrupt` can
        return while that frame is still in progress.

        Returns the number of stale frames dropped.
        """
        async with self._lifecycle_lock:
            if self.state != RuntimeState.RUNNING:
                return 0

            if self._owns_generation:
                self.generation.bump()

            if not self._holds_current_work():
                await self._cancel_worker()
                self.task = asyncio.create_task(self._run())

            return self._drop_stale_input()

    async def _run(self) -> None:
        while True:
            frame = await self.input_queue.get()

            try:
                if self.generation.is_stale(frame.generation):
                    continue

                self._inflight = frame

                for output in await self.processor.process(frame=frame):
                    await self._emit(output, source=frame)

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                self._fail(exc)
                return

            finally:
                self._inflight = None
                self.input_queue.task_done()

    async def _emit(self, output: Frame, source: Frame) -> None:
        output.generation = source.generation
        await self.output_queue.put(output)

    def _fail(self, exc: Exception) -> None:
        self.error = exc
        self.state = RuntimeState.FAILED

    async def _wait_for_empty(self, timeout: float | None) -> bool:
        try:
            if timeout is None:
                await self.input_queue.join()
            else:
                await asyncio.wait_for(self.input_queue.join(), timeout=timeout)
        except TimeoutError:
            return False

        return True

    async def _shutdown(self) -> None:
        self.state = RuntimeState.STOPPING
        await self._cancel_worker()
        self.state = RuntimeState.STOPPED

    async def _cancel_worker(self) -> None:
        if self.task is None:
            return

        self.task.cancel()

        try:
            await self.task
        except asyncio.CancelledError:
            pass

        self.task = None

    def _holds_current_work(self) -> bool:
        # A dequeued frame is invisible to the sweep, so this one has to survive.
        if self._inflight is None:
            return False

        return not self.generation.is_stale(self._inflight.generation)

    def _drop_stale_input(self) -> int:
        kept: list[Frame] = []
        dropped = 0

        while not self.input_queue.empty():
            try:
                frame = self.input_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            self.input_queue.task_done()

            if self.generation.is_stale(frame.generation):
                dropped += 1
            else:
                kept.append(frame)

        for frame in kept:
            self.input_queue.put_nowait(frame)

        return dropped
