import pytest

from rivet.frames import TextFrame
from rivet.processors.slow import SlowProcessor
from rivet.runtime import ProcessorRuntine


@pytest.mark.asyncio
async def test_processor_runtime():
    runtime = ProcessorRuntine(SlowProcessor())

    await runtime.start()

    await runtime.push(TextFrame("hello"))

    await runtime.queue.join()
