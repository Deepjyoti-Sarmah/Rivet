import asyncio

import pytest

from rivet.frames import TextFrame
from rivet.processor import Processor
from rivet.runtime import ProcessorRuntime


class BlockingProcessor(Processor):
    def __init__(self, name: str):
        self.name = name
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def process(self, frame) -> list:
        print(f"{self.name}: START {frame}")

        self.started.set()

        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            print(f"{self.name}: CANCELLED {frame}")
            self.cancelled.set()
            raise

        return [frame]

class ForwardingBlockingProcessor(Processor):
    def __init__(self, name: str):
        self.name = name
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def process(self, frame) -> list:
        print(f"{self.name}: START {frame}")

        self.started.set()

        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            print(f"{self.name}: CANCELLED {frame}")
            self.cancelled.set()
            raise

        return [frame]

@pytest.mark.asyncio
async def test_interrupt_propagates_downstream():
    processor_a = BlockingProcessor("A")
    processor_b = BlockingProcessor("B")
    processor_c = BlockingProcessor("C")

    runtime_c = ProcessorRuntime(processor_c)
    runtime_b = ProcessorRuntime(
        processor_b,
        output_queue=runtime_c.input_queue,
    )
    runtime_a = ProcessorRuntime(
        processor_a,
        output_queue=runtime_b.input_queue,
    )

    await runtime_c.start()
    await runtime_b.start()
    await runtime_a.start()

    await runtime_a.push(TextFrame("hello"))

    await processor_a.started.wait()

    await runtime_a.interrupt()

    assert processor_a.cancelled.is_set()

    await runtime_a.stop()
    await runtime_b.stop()
    await runtime_c.stop()
