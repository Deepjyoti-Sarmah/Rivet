import asyncio

import pytest

from rivet.frames import TextFrame
from rivet.processors.exclamation import ExclamationProcessor
from rivet.processors.uppercase import UppercaseProcessor
from rivet.runtime import ProcessorRuntime


@pytest.mark.asyncio
async def test_two_processor_pipeline():
    queue_2: asyncio.Queue[TextFrame] = asyncio.Queue()
    queue_3: asyncio.Queue[TextFrame] = asyncio.Queue()

    uppercase = ProcessorRuntime(
        UppercaseProcessor(),
        output_queue=queue_2,
    )

    exclamation = ProcessorRuntime(
        ExclamationProcessor(),
        output_queue=queue_3,
    )

    await uppercase.start()
    await exclamation.start()

    async def forward() -> None:
        while True:
            frame = await queue_2.get()

            try:
                await exclamation.push(frame)
            finally:
                queue_2.task_done()

    forward_task = asyncio.create_task(forward())

    await uppercase.push(TextFrame("hello"))

    await uppercase.input_queue.join()
    await queue_2.join()
    await exclamation.input_queue.join()

    result = await queue_3.get()

    try:
        assert result.text == "HELLO!"
    finally:
        queue_3.task_done()

    forward_task.cancel()

    try:
        await forward_task
    except asyncio.CancelledError:
        pass

    await uppercase.stop()
    await exclamation.stop()
