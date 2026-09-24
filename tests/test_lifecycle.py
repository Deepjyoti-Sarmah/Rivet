import asyncio

import pytest
from conftest import (
    ExplodingProcessor,
    GatedProcessor,
    RecordingProcessor,
    SlowCancelProcessor,
)

from rivet.frames import TextFrame
from rivet.pipeline import Pipeline
from rivet.runtime import ProcessorRuntime, RuntimeState


@pytest.mark.asyncio
async def test_concurrent_stop_is_idempotent():
    runtime = ProcessorRuntime(GatedProcessor())
    await runtime.start()

    # Shutdown is reached from several paths at once by design: normal exit,
    # signal handler, error handler. They do not coordinate.
    await asyncio.gather(runtime.stop(), runtime.stop(), runtime.stop())

    assert runtime.state is RuntimeState.STOPPED
    assert runtime.task is None


@pytest.mark.asyncio
async def test_state_is_stopping_while_the_worker_unwinds():
    """A state name must be true for the whole time it is set.

    Saying STOPPED before the worker has finished unwinding tells an observer
    the queues are safe to tear down while the worker is still writing to them.
    """
    processor = SlowCancelProcessor("A")
    runtime = ProcessorRuntime(processor)
    await runtime.start()

    await runtime.push(TextFrame("one"))
    await processor.started.wait()

    stopping = asyncio.create_task(runtime.stop())
    await processor.cancel_started.wait()

    # The worker is mid-unwind: not RUNNING any more, not gone yet.
    assert runtime.state is RuntimeState.STOPPING

    processor.release_cancel.set()
    await stopping

    assert runtime.state is RuntimeState.STOPPED


@pytest.mark.asyncio
async def test_drain_returns_true_when_queue_empties():
    processor = RecordingProcessor("A")
    runtime = ProcessorRuntime(processor)
    await runtime.start()

    await runtime.push(TextFrame("one"))
    await runtime.push(TextFrame("two"))

    assert await runtime.drain() is True
    assert runtime.state is RuntimeState.STOPPED

    # drain() means finish the work, not discard it.
    assert [frame.text for frame in processor.seen] == ["one", "two"]


@pytest.mark.asyncio
async def test_drain_times_out_on_a_stuck_processor():
    """Without a bound this test hangs the suite -- which is the bug."""
    processor = GatedProcessor()
    runtime = ProcessorRuntime(processor)
    await runtime.start()

    await runtime.push(TextFrame("stuck"))
    await processor.started.wait()

    # The gate is never opened, so the queue can never empty.
    assert await runtime.drain(timeout=0.1) is False

    # Gave up, but still shut down rather than hanging forever.
    assert runtime.state is RuntimeState.STOPPED


@pytest.mark.asyncio
async def test_drain_rejects_new_work():
    processor = GatedProcessor()
    runtime = ProcessorRuntime(processor)
    await runtime.start()

    await runtime.push(TextFrame("one"))
    await processor.started.wait()

    draining = asyncio.create_task(runtime.drain(timeout=0.2))
    await asyncio.sleep(0)  # let drain() reach DRAINING

    # The entrance closes before the wait begins, or a busy producer could keep
    # join() from ever returning.
    with pytest.raises(RuntimeError):
        await runtime.push(TextFrame("two"))

    await draining


@pytest.mark.asyncio
async def test_stop_during_drain_wins_cleanly():
    processor = GatedProcessor()
    runtime = ProcessorRuntime(processor)
    await runtime.start()

    await runtime.push(TextFrame("one"))
    await processor.started.wait()

    # Both are legitimate; whoever gets there first owns the shutdown.
    draining = asyncio.create_task(runtime.drain())
    await asyncio.sleep(0)

    await runtime.stop()

    assert runtime.state is RuntimeState.STOPPED

    # drain() must not hang once stop() has taken over.
    await asyncio.wait_for(draining, timeout=1.0)


@pytest.mark.asyncio
async def test_processor_exception_marks_runtime_failed():
    processor = ExplodingProcessor()
    runtime = ProcessorRuntime(processor)
    await runtime.start()

    await runtime.push(TextFrame("boom"))
    await processor.started.wait()
    await asyncio.sleep(0)

    assert runtime.state is RuntimeState.FAILED
    assert isinstance(runtime.error, ValueError)

    # A dead worker must stop claiming it can take work.
    with pytest.raises(RuntimeError):
        await runtime.push(TextFrame("another"))


@pytest.mark.asyncio
async def test_stop_on_failed_runtime_is_a_noop():
    processor = ExplodingProcessor()
    runtime = ProcessorRuntime(processor)
    await runtime.start()

    await runtime.push(TextFrame("boom"))
    await processor.started.wait()
    await asyncio.sleep(0)

    await runtime.stop()

    # FAILED is not overwritten -- why it died still matters after shutdown.
    assert runtime.state is RuntimeState.FAILED
    assert isinstance(runtime.error, ValueError)


@pytest.mark.asyncio
async def test_start_twice_raises():
    runtime = ProcessorRuntime(RecordingProcessor("A"))
    await runtime.start()

    with pytest.raises(RuntimeError):
        await runtime.start()

    await runtime.stop()


@pytest.mark.asyncio
async def test_pipeline_drain_finishes_every_stage():
    processor_a = RecordingProcessor("A")
    processor_b = RecordingProcessor("B")

    pipeline = Pipeline([processor_a, processor_b])
    await pipeline.start()

    await pipeline.push(TextFrame("one"))
    await pipeline.push(TextFrame("two"))

    assert await pipeline.drain() is True

    for runtime in pipeline.runtimes:
        assert runtime.state is RuntimeState.STOPPED

    # Drained source -> sink, so work reached the far end rather than being
    # stranded in a stage that was shut down too early.
    assert [frame.text for frame in processor_b.seen] == ["one", "two"]


@pytest.mark.asyncio
async def test_pipeline_drain_reports_failure_if_any_stage_times_out():
    processor_a = RecordingProcessor("A")
    processor_b = GatedProcessor("B")

    pipeline = Pipeline([processor_a, processor_b])
    await pipeline.start()

    await pipeline.push(TextFrame("one"))
    await processor_b.started.wait()

    assert await pipeline.drain(timeout=0.1) is False

    await pipeline.stop()


@pytest.mark.asyncio
async def test_pipeline_stop_is_idempotent():
    pipeline = Pipeline([GatedProcessor()])
    await pipeline.start()

    await asyncio.gather(pipeline.stop(), pipeline.stop())

    for runtime in pipeline.runtimes:
        assert runtime.state is RuntimeState.STOPPED
