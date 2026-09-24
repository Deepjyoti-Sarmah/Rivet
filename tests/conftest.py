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


class GatedProcessor(Processor):
    """Holds each frame until the gate is opened. No sleeps, no timing luck."""

    def __init__(self, name: str = "gated") -> None:
        self.name = name
        self.gate = asyncio.Event()
        self.started = asyncio.Event()
        self.seen: list[Frame] = []

    async def process(self, frame: Frame) -> list[Frame]:
        self.seen.append(frame)
        self.started.set()
        await self.gate.wait()
        return [frame]


class ExplodingProcessor(Processor):
    """Raises on every frame."""

    def __init__(self, name: str = "boom") -> None:
        self.name = name
        self.started = asyncio.Event()

    async def process(self, frame: Frame) -> list[Frame]:
        self.started.set()
        raise ValueError("processor exploded")


class SlowCancelProcessor(Processor):
    """Stalls inside its CancelledError handler, holding the sweep open."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.started = asyncio.Event()
        self.cancel_started = asyncio.Event()
        self.release_cancel = asyncio.Event()
        self.seen: list[Frame] = []

    async def process(self, frame: Frame) -> list[Frame]:
        self.seen.append(frame)
        self.started.set()

        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            self.cancel_started.set()
            await self.release_cancel.wait()
            raise

        return [frame]
