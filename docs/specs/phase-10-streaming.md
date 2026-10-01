# Spec — Phase 10: Streaming processors

**Status:** draft
**Depends on:** phase 9 (generations), phase 8 (lifecycle)

---

## Problem

```python
async def process(self, frame) -> list[Frame]:
```

A list is a finished thing. Returning it requires producing all of it first.

```
LLM producing 50 tokens

token 1 exists       40ms
token 50 exists     800ms
list returned       800ms
TTS can start       800ms
```

Token 1 waited 760ms for its siblings. `return` is terminal, so "emit and keep
going" is not expressible in this signature — not awkward, impossible.

The cost is the gap between an agent that starts speaking in 200ms and one that
starts in a second.

## Contract

```python
async def process(self, frame: Frame) -> list[Frame] | AsyncIterator[Frame]:
```

Two supported forms:

```python
async def process(self, frame):          # batch, unchanged
    return [TextFrame("done")]

async def process(self, frame):          # streaming
    for token in tokens:
        yield TextFrame(token)
```

The runtime detects which by inspecting the *function*, not its result:

```python
inspect.isasyncgenfunction(processor.process)
```

Calling an async generator function returns a generator object with no `await`,
so `await process(frame)` raises `TypeError` on one. Detection must happen before
the call, and is done once at construction.

### Stamping

A yielded frame is stamped by the runtime, from the frame that caused it, before
it reaches the output queue. That is the same rule `push` follows, in the same
order:

```python
async def _emit(self, output: Frame, source: Frame) -> None:
    output.generation = source.generation
    await self.output_queue.put(output)
```

Three paths stamp in this runtime: `Pipeline.push` for work entering the chain,
`ProcessorRuntime.push` when the runtime owns the counter, and `_emit` for
everything a processor produces. Each stamps exactly once — the first two are
mutually exclusive — and each stamps before the frame is queued, so a frame that
waits on a full queue cannot inherit the turn that released it.

A processor cannot forge a generation. Whatever it set on a yielded frame is
overwritten, which is what makes a stream safe to trust: its frames are as much
the runtime's as the batch path's.

## Invariants

```
I1  output.generation == input.generation, for both forms
I2  the generator is closed when iteration ends for any reason,
    including cancellation
I3  a stream stops emitting once its turn is superseded
I4  a full downstream queue suspends the generator mid-function
I5  frames from one stream reach the output queue in yield order
I6  input_queue.task_done() is called exactly once per frame, including
    when a stream is cancelled partway
I7  every emitted frame carries its source frame's generation, stamped
    by the runtime before the put, whatever the processor set
```

## Acceptance

```
A1  a consumer reads frame N before the processor has produced N+1
A2  a streaming processor's generator runs its finally block when the
    runtime is stopped mid-stream
A3  bumping the generation mid-stream stops further output from that stream
A4  a full output queue blocks the processor between yields
A5  existing list-returning processors behave exactly as before
A6  an exception raised mid-stream marks the runtime FAILED and retains
    the exception
A7  a streaming processor that yields a frame carrying a pre-set
    generation does not put that frame in the turn it chose
```

Each gets one test, named for its id.

## Non-goals

```
replacing list[Frame]           both forms stay valid; forcing a one-step
                                transform through a generator is ceremony

concurrent streams              one worker per stage still means one frame at
                                a time; ordering across parallel streams is
                                phase 11

partial-output marking          a stream cut short emits no marker saying so;
                                needs the error model, phase 13

stream-level backpressure        beyond what awaiting put() already gives
    tuning
```

## Risks

```
aclose() during cancellation    awaiting inside a finally while CancelledError
                                is propagating can itself be interrupted

detection false negative        a processor wrapping its generator in a plain
                                async def returns an iterator the runtime will
                                treat as a list and fail to iterate

stamp taken after the put      a stream is a long-lived window, so a yield can
                                land while the queue is full. Stamping after
                                the await admits the frame to the turn that
                                freed the slot, and drain() then waits for a
                                frame that was already stale
```

Each needs a test or an explicit decision before this spec leaves draft.
