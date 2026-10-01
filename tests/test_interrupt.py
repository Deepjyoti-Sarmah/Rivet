import asyncio

import pytest
from conftest import BlockingProcessor as ConftestBlockingProcessor
from conftest import RecordingProcessor

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


@pytest.mark.asyncio
async def test_standalone_runtime_processes_frames_after_interrupt():
    """A runtime that owns its counter must stamp what it is pushed.

    Without a stamp every frame keeps generation 0, so the first interrupt
    makes all later work stale and the runtime silently discards it.
    """
    processor = RecordingProcessor("A")

    runtime = ProcessorRuntime(processor)

    await runtime.start()

    await runtime.interrupt()
    await runtime.push(TextFrame("new turn"))

    await asyncio.wait_for(processor.done.wait(), timeout=1.0)

    assert [frame.text for frame in processor.seen] == ["new turn"]

    await runtime.stop()


@pytest.mark.asyncio
async def test_standalone_push_stamps_before_waiting_for_space():
    """A frame pushed before the bump must not ride the bump into the new turn.

    The stamp is taken before the put, because a push that blocks on a full queue
    can be released by the sweep that the same interrupt performs. Stamping after
    the wait would admit the frame to the turn it was pushed in front of.
    """
    processor = ConftestBlockingProcessor("A")

    runtime = ProcessorRuntime(processor, max_queue_size=2)

    await runtime.start()

    await runtime.push(TextFrame("in flight"))

    # Occupies both remaining slots, so the push below has to wait.
    await runtime.push(TextFrame("queued 1"))
    await runtime.push(TextFrame("queued 2"))

    late = asyncio.create_task(runtime.push(TextFrame("late")))

    # One scheduling yield, not a timing sleep: the task above runs until it
    # blocks in put, because the queue is full and there is nothing else to do.
    # Without it the push would not start until after the bump and would be
    # stamped with the new generation.
    await asyncio.sleep(0)

    dropped = await runtime.interrupt()
    await asyncio.wait_for(late, timeout=1.0)

    # Reached only if the worker got past "late" and took the next frame.
    processor.started.clear()
    await runtime.push(TextFrame("sentinel"))
    await asyncio.wait_for(processor.started.wait(), timeout=1.0)

    seen = [frame.text for frame in processor.seen]

    assert dropped == 2, f"expected the two queued frames dropped, got {dropped}"
    assert "late" not in seen, f"a frame pushed before the bump was processed: {seen}"
    assert seen == ["in flight", "sentinel"], seen

    await runtime.stop()
