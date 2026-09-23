# Phase 2 — The processor runtime

**Status:** ✅ done
**Code:** `src/rivet/runtime.py`, `src/rivet/pipeline.py`
**Tests:** `tests/test_pipeline_runtime.py`, `tests/test_runtime_pipeline.py`

---

## WHY

Phase 1 gave us `process(frame) -> list[Frame]`. Now: who calls it, and when?

The naive version is a loop that awaits each stage in turn:

```python
frames = [frame]
for processor in processors:
    frames = [out for f in frames for out in await processor.process(f)]
```

This is correct and completely useless for realtime. It is a **pipeline in name
only** — at any instant exactly one processor is running and the rest are idle.
With STT at 100ms, LLM at 800ms, and TTS at 200ms, every utterance costs the full
1.1s serial sum, and the STT stage sits idle for a second while it could have been
transcribing the *next* chunk of speech.

Worse, it couples the caller to the whole chain. Nothing can be cancelled, paused,
or measured independently.

## WHAT

**Separate *what to do* from *how to run it continuously*.**

| | Owns |
| --- | --- |
| `Processor` | the transformation — one frame in, frames out |
| `ProcessorRuntime` | input queue, output queue, a worker task, lifecycle |
| `Pipeline` | constructing runtimes and wiring them together |

`ProcessorRuntime` wraps a processor in a permanently-running worker:

```python
async def _run(self) -> None:
    while True:
        frame = await self.input_queue.get()
        try:
            output_frames = await self.processor.process(frame=frame)
            for output in output_frames:
                await self.output_queue.put(output)
        finally:
            self.input_queue.task_done()
```

Each stage gets its **own task**, so stages run concurrently:

```
A.worker  ──[frame 3]──▶
B.worker  ──[frame 2]──▶     all three busy, on different frames
C.worker  ──[frame 1]──▶
```

Throughput becomes governed by the *slowest* stage rather than the *sum* of
stages. That is the whole point of a pipeline.

## HOW

### `asyncio.create_task` vs `await`

The distinction the whole design rests on:

```python
await self._run()                    # runs it NOW, blocks until done — never returns
asyncio.create_task(self._run())     # schedules it, returns immediately
```

`start()` uses `create_task` so control returns to the caller while the worker
keeps running in the background. `await` would hang the caller forever, since
`_run` is an infinite loop.

### `await queue.get()` does not block the event loop

The most common asyncio misconception. When the queue is empty, `await get()`
**suspends this task** and hands control back to the event loop, which runs other
tasks. The loop is free; only this coroutine is parked. When a frame arrives, the
task is rescheduled.

That is why three "infinite loops" can coexist without pinning a CPU.

### Queues give temporal decoupling

```
producer ──put()──▶ [ queue ] ──get()──▶ consumer
```

The producer doesn't wait for processing to complete; it hands off and moves on.
Producer and consumer run at independent rates — within limits, which is exactly
what Phase 3 is about.

## TRADE-OFFS

**One worker per stage means strict ordering within a stage.** Frames are processed
one at a time, in arrival order. This is not an oversight — audio cannot be
casually reordered, and a transcript assembled out of order is gibberish. We give
up per-stage parallelism to keep ordering for free.

**Concurrency comes from the number of stages, not from workers per stage.** A
3-stage pipeline has 3 frames in flight, maximum.

**`Pipeline` uses forwarder tasks.** Stage *i*'s output goes to an intermediate
queue, and a separate `_forward` task moves frames to stage *i+1*. This works, but
creates a moment where a frame is held by the forwarder and is in *neither* queue —
which becomes an active problem in Phase 7, and is removed there.

## FAILURE MODES

- **A processor exception kills the worker silently.** `_run` doesn't catch
  non-cancellation exceptions, so they propagate out of the task and are stored in
  it, never retrieved. The runtime still reports `RUNNING`; `push()` still accepts
  frames that will never be processed. Addressed in Phase 7 (visibility) and
  Phase 13 (a real error model).
- **The terminal output queue is unbounded**, breaking the backpressure chain at
  its very end. See Phase 3.
- **No restart path.** Once stopped, a runtime cannot be started again —
  `start()` raises from any state but `CREATED`.

## WHAT WE LEARNED

> `Processor` = what to do. `ProcessorRuntime` = how to run it continuously.

Keeping these apart is what lets us later add backpressure, cancellation,
interruption, and metrics without touching a single line of business logic. Every
subsequent phase is a change to the runtime; the processors have not changed since
Phase 1.
