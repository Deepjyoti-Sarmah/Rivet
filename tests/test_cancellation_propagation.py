import asyncio

import pytest
from conftest import BlockingProcessor, RecordingProcessor, SlowCancelProcessor

from rivet.frames import TextFrame
from rivet.pipeline import Pipeline


@pytest.mark.asyncio
async def test_interrupt_clears_every_stage():
    processor_a = RecordingProcessor("A")
    processor_b = BlockingProcessor("B")
    processor_c = BlockingProcessor("C")

    pipeline = Pipeline([processor_a, processor_b, processor_c])
    await pipeline.start()

    runtime_a, runtime_b, runtime_c = pipeline.runtimes

    await pipeline.push(TextFrame("old 1"))
    await pipeline.push(TextFrame("old 2"))
    await pipeline.push(TextFrame("old 3"))

    await processor_b.started.wait()
    await runtime_a.input_queue.join()

    # Setup check: B holds one frame, two are queued behind it.
    assert runtime_b.input_queue.qsize() == 2

    dropped = await pipeline.interrupt()

    assert processor_b.cancelled.is_set(), "B kept working on the dead turn"
    assert dropped == 2, f"expected 2 stale frames dropped, got {dropped}"

    assert runtime_a.input_queue.empty()
    assert runtime_b.input_queue.empty()
    assert runtime_c.input_queue.empty()
    assert pipeline.output_queue.empty()

    await pipeline.stop()


@pytest.mark.asyncio
async def test_pipeline_works_after_interrupt():
    processor_a = RecordingProcessor("A")
    processor_b = BlockingProcessor("B")

    pipeline = Pipeline([processor_a, processor_b])
    await pipeline.start()

    await pipeline.push(TextFrame("old turn"))
    await processor_b.started.wait()

    await pipeline.interrupt()

    # The interrupt must leave the pipeline usable, not dead.
    for runtime in pipeline.runtimes:
        assert runtime.state.value == "running"

    # An Event stays set once set; reset it or the wait below returns instantly.
    processor_b.started.clear()

    await pipeline.push(TextFrame("new turn"))
    await processor_b.started.wait()

    assert processor_b.seen[-1].text == "new turn"

    await pipeline.stop()


@pytest.mark.asyncio
async def test_new_turn_frame_survives_interrupt_sweep():
    """A frame pushed mid-sweep belongs to the new turn and must survive.

    Closed in phase 9: the sweep reads frame.generation instead of assuming
    anything it finds in an inbox is stale.
    """
    processor_a = SlowCancelProcessor("A")

    pipeline = Pipeline([processor_a])
    await pipeline.start()

    await pipeline.push(TextFrame("old turn"))
    await processor_a.started.wait()

    # Stale work queued behind it, so the sweep has something to discard. Without
    # this the test passes even if nothing is ever dropped.
    await pipeline.push(TextFrame("old queued"))

    # Hold the sweep open inside A's cancellation handler.
    sweep = asyncio.create_task(pipeline.interrupt())
    await processor_a.cancel_started.wait()

    # Reset before pushing: once released, the new worker can pick the frame up
    # immediately, and a clear() after that would wipe the signal we wait on.
    processor_a.started.clear()

    # The new turn arrives mid-sweep and lands in A's inbox, which the sweep
    # is about to flush.
    await pipeline.push(TextFrame("new turn"))

    processor_a.release_cancel.set()
    dropped = await sweep

    # The old turn's queued frame went; the new turn's did not.
    assert dropped == 1, f"expected the stale frame dropped, got {dropped}"

    await asyncio.wait_for(processor_a.started.wait(), timeout=1.0)

    assert processor_a.seen[-1].text == "new turn"
    assert "old queued" not in [frame.text for frame in processor_a.seen]

    await pipeline.stop()
