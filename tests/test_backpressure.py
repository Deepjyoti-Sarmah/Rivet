import asyncio

import pytest

from rivet.frames import Frame, TextFrame
from rivet.pipeline import Pipeline
from rivet.processor import Processor


class GatedProcessor(Processor):
    """Holds each frame until the gate is opened. No sleeps, no timing luck."""

    def __init__(self) -> None:
        self.gate = asyncio.Event()
        self.started = asyncio.Event()

    async def process(self, frame: Frame) -> list[Frame]:
        self.started.set()
        await self.gate.wait()
        return [frame]


@pytest.mark.asyncio
async def test_producer_blocks_when_capacity_is_exhausted():
    processor = GatedProcessor()
    pipeline = Pipeline([processor], max_queue_size=1)

    await pipeline.start()

    # Frame 1 is pulled out of the inbox and stuck inside the processor.
    await pipeline.push(TextFrame("1"))
    await processor.started.wait()

    # Frame 2 fills the inbox (maxsize=1).
    await pipeline.push(TextFrame("2"))

    # Frame 3 has nowhere to go. The producer must block.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(pipeline.push(TextFrame("3")), timeout=0.2)

    processor.gate.set()
    await pipeline.stop()


@pytest.mark.asyncio
async def test_producer_resumes_when_consumer_drains():
    processor = GatedProcessor()
    pipeline = Pipeline([processor], max_queue_size=1)

    await pipeline.start()

    await pipeline.push(TextFrame("1"))
    await processor.started.wait()
    await pipeline.push(TextFrame("2"))

    # Blocked, as above.
    pending = asyncio.create_task(pipeline.push(TextFrame("3")))
    await asyncio.sleep(0)
    assert not pending.done()

    # Let work flow again; the blocked push must complete on its own.
    processor.gate.set()
    await asyncio.wait_for(pending, timeout=1.0)

    assert pending.done()

    await pipeline.stop()
