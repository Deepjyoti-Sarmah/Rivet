import asyncio

import pytest

from rivet.frames import TextFrame
from rivet.processors.slow import SlowProcessor
from rivet.runtime import ProcessorRuntime


@pytest.mark.asyncio
async def test_drain_finishes_existing_work():
    output_queue: asyncio.Queue[TextFrame] = asyncio.Queue()

    runtime = ProcessorRuntime(
        SlowProcessor(),
        output_queue=output_queue,
    )
    await runtime.start()

    await runtime.push(TextFrame("one"))
    await runtime.push(TextFrame("two"))

    await runtime.drain()

    assert runtime.state.value == "stopped"

    first = await output_queue.get()
    output_queue.task_done()

    second = await output_queue.get()
    output_queue.task_done()

    assert first.text == "one"
    assert second.text == "two"


@pytest.mark.asyncio
async def test_drain_rejects_new_work():
    runtime = ProcessorRuntime(SlowProcessor())

    await runtime.start()

    await runtime.push(TextFrame("one"))

    drain_task = asyncio.create_task(runtime.drain())

    await asyncio.sleep(0)

    with pytest.raises(RuntimeError):
        await runtime.push(TextFrame("two"))

    await drain_task
