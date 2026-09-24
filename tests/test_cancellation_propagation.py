import pytest
from conftest import BlockingProcessor, RecordingProcessor

from rivet.frames import TextFrame
from rivet.runtime import ProcessorRuntime


@pytest.mark.asyncio
async def test_interrupt_at_a_does_not_reach_b_or_c():
    """Interrupting A leaves B working on the dead turn and C's inbox full.

    This is the bug phase 7 closes. Expected to FAIL until Pipeline.interru
    exists.
    """
    processor_a = RecordingProcessor("A")
    processor_b = BlockingProcessor("B")
    processor_c = BlockingProcessor("C")

    runtime_c = ProcessorRuntime(processor_c)
    runtime_b = ProcessorRuntime(processor_b, output_queue=runtime_c.input_queue)
    runtime_a = ProcessorRuntime(processor_a, output_queue=runtime_b.input_queue)

    await runtime_c.start()
    await runtime_b.start()
    await runtime_a.start()

    # A completes and emits; B picks it up and block
    await runtime_a.push(TextFrame("old turn"))
    await processor_b.started.wait()

    # Queue two more stale frames behind B, destined for C.
    await runtime_b.push(TextFrame("stale 1"))
    await runtime_b.push(TextFrame("stale 2"))

    await runtime_a.interrupt()

    assert processor_b.cancelled.is_set(), "B kept working on the dead turn"
    assert runtime_b.input_queue.empty(), "stale frames still queued for B"
    assert runtime_c.input_queue.empty(), "stale frames still queued for C"

    await runtime_a.stop()
    await runtime_b.stop()
    await runtime_c.stop()
