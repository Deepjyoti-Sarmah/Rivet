import asyncio

import pytest

from rivet.frames import Frame, TextFrame
from rivet.processor import Processor
from rivet.runtime import ProcessorRuntime


class BlockingProcessor(Processor):
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def process(self, frame: Frame) -> list[Frame]:
        print(f"START: {frame}")

        self.started.set()

        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            print(f"CANCELLED: {frame}")
            self.cancelled.set()
            raise

        return [frame]


@pytest.mark.asyncio
async def test_interrupt_cancels_current_frame():
    processor = BlockingProcessor()

    runtime = ProcessorRuntime(processor=processor)

    await runtime.start()

    await runtime.push(TextFrame("hello"))

    # wait untill the process is actully processing "hello".
    await processor.started.wait()

    await runtime.interrupt()

    assert processor.cancelled.is_set()
