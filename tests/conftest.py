import asyncio

from rivet.frames import Frame
from rivet.processor import Processor


class RecordingProcessor(Processor):
    """Completes immediately and forwards the frame. Records what it saw."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.seen: list[Frame] = []
        self.done = asyncio.Event()

    async def process(self, frame: Frame) -> list[Frame]:
        self.seen.append(frame)
        self.done.set()
        return [frame]


class BlockingProcessor(Processor):
    """Blocks forever once it starts. Signals start and cancellation."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.seen: list[Frame] = []

    async def process(self, frame: Frame) -> list[Frame]:
        self.seen.append(frame)
        self.started.set()

        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            self.cancelled.set()
            raise

        return [frame]
