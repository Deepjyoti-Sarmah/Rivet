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


@pytest.mark.asyncio
async def test_interrupt_keeps_runtime_running():
    processor = BlockingProcessor()

    runtime = ProcessorRuntime(processor)

    await runtime.start()

    await runtime.push(TextFrame("hello"))

    await processor.started.wait()

    await runtime.interrupt()

    assert runtime.state.value == "running"


@pytest.mark.asyncio
async def test_interrupt_clears_queued_frames():
    processor = BlockingProcessor()

    runtime = ProcessorRuntime(
        processor,
        max_queue_size=10,
    )

    await runtime.start()

    await runtime.push(TextFrame("A"))

    # Make sure A is currently being processed.
    await processor.started.wait()

    # These should remain queued.
    await runtime.push(TextFrame("B"))
    await runtime.push(TextFrame("C"))
    await runtime.push(TextFrame("D"))

    await runtime.interrupt()

    assert runtime.input_queue.empty()
