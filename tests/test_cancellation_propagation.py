import pytest
from conftest import BlockingProcessor, RecordingProcessor

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
