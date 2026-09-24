# Phase 9 — Frame metadata (generation ids)

**Status:** ✅ done
**Code:** `src/rivet/frames.py`, `src/rivet/runtime.py`, `src/rivet/pipeline.py`
**Tests:** `tests/test_cancellation_propagation.py`
**Closes:** the race left open by [phase 7](phase-07-cancellation-propagation.md)

---

## WHY

Phase 7's sweep clears a chain by asking, of each frame it finds:

> is this stale?

and answering by **timing** — "it is in an inbox while I am cleaning, so yes."

That answer is wrong for anything that arrived a moment ago.

```
sweep:   cancel A ──────────── flush A's inbox
                    │
new:          push "Tokyo" ──▶ lands in the inbox
                                        │
                                   deleted
```

The user interrupts, is heard, and gets silence.

Reordering does not help:

| Order | What breaks |
| --- | --- |
| cancel → flush | a new arrival lands between them |
| flush → cancel | the dying worker emits into the flushed inbox |
| lock out `push` | the new turn is rejected instead of lost |

The window exists because, looking at a frame in a queue:

```
TextFrame("...")    Paris?
TextFrame("...")    Tokyo?
```

**nothing distinguishes them.** Same class, same fields. The information the sweep
needs does not exist, so no amount of cleverness in the sweep can recover it.

## WHAT

Put the answer on the frame.

```python
@dataclass(slots=True)
class Frame:
    type: FrameType = field(init=False)
    generation: int = field(init=False, default=0)
```

```
push "Paris"  →  gen 10
interrupt     →  gen becomes 11
push "Tokyo"  →  gen 11

flush:  frame.generation < 11  →  discard
        frame.generation == 11 →  keep
```

Timing and ordering stop mattering. The frame carries its own verdict.

> **cancel-by-timing → invalidate-by-label**

## HOW

### A shared counter

```python
class Generation:
    def __init__(self) -> None:
        self.value = 0

    def bump(self) -> int:
        self.value += 1
        return self.value
```

A plain `int` will not do. Every stage must see the *same* counter, and Python
ints are immutable — each runtime would hold a private copy. One shared mutable
object instead.

`Pipeline` owns one and passes it to every runtime.

### Stamp on entry, bump before sweeping

```python
    async def push(self, frame: Frame) -> None:
        frame.generation = self.generation.value
        await self.runtimes[0].push(frame=frame)

    async def interrupt(self) -> int:
        async with self._interrupt_lock:
            self.generation.bump()     # FIRST
            for runtime in self.runtimes:
                dropped += await runtime.interrupt()
            dropped += self._flush_output()
```

Bump-first is the whole trick:

```
bump ──▶ sweep ────────────────────▶
  │              │
  gen = 11    push "Tokyo" → stamped 11 → survives the flush
```

Bump last and Tokyo is stamped 10, indistinguishable from Paris, deleted.

Everything already in the system is stale **by definition** the instant the
counter moves. Nothing has to be enumerated or tracked.

### Flush became a filter

Before — delete everything:

```python
while not self.input_queue.empty():
    self.input_queue.get_nowait()
```

After — drain, judge, re-queue the survivors:

```python
    def _flush_input(self) -> int:
        kept, dropped = [], 0

        while not self.input_queue.empty():
            frame = self.input_queue.get_nowait()
            self.input_queue.task_done()

            if frame.generation < self.generation.value:
                dropped += 1
            else:
                kept.append(frame)

        for frame in kept:
            self.input_queue.put_nowait(frame)

        return dropped
```

Draining in order and re-queueing in order preserves ordering.

### The worker also checks

A frame can be dequeued before the sweep reaches it:

```python
frame = await self.input_queue.get()
if frame.generation < self.generation.value:
    continue
```

Two independent checks — at dequeue and at flush — because a frame can be in
either place when the counter moves.

### Outputs inherit the stamp

Processors build fresh objects:

```python
return [TextFrame(frame.text.upper())]     # generation defaults to 0
```

Left alone, every output would look ancient and be discarded immediately. The
runtime carries the stamp across:

```python
output.generation = frame.generation
```

**Processors never see generations.** No processor changed in this phase — the
same decoupling Phase 2 bought.

### Who bumps

Whoever initiates the interrupt.

```python
self._owns_generation = generation is None
...
if self._owns_generation:
    self.generation.bump()
```

Standalone, the runtime owns its counter and bumps it. Inside a pipeline the
counter is shared and only `Pipeline` bumps — otherwise it would advance once per
stage, and a 3-stage sweep would jump 3 generations.

This surfaced as a test failure: `test_interrupt_clears_queued_frames` uses a bare
`ProcessorRuntime`, where nothing was bumping, so `0 < 0` was false and nothing was
ever stale.

### `init=False` on the field

```python
generation: int = field(init=False, default=0)
```

Dataclasses forbid a field without a default following one with a default. A plain
`generation: int = 0` on the base would break every subclass:

```python
TextFrame(text: str)      # no default, follows generation=0  → TypeError
```

`init=False` excludes it from `__init__`, so the ordering rule never applies and
`TextFrame("hello")` still works.

## TRADE-OFFS

**A counter, not a UUID.** Comparison is `<`, which gives "older than" for free.
A UUID would need a separate ordering. The cost is that it is meaningless across
processes — fine while a pipeline is one object in one loop, wrong the moment a
turn id has to survive a transport boundary (Phase 15).

**Stamped centrally, not by processors.** `Pipeline.push` is the only place a
generation originates. Processors cannot forge one, and cannot forget one.

**Frames are mutable.** `output.generation = ...` writes to a frame a processor
just built. Acceptable because the runtime stamps before anything downstream can
observe it, but it means frames are not safe to share between stages by reference.

**One counter per pipeline, not per branch.** Fine for a linear chain. Phase 11
(routing) will need to decide whether parallel branches share a turn.

**Only `generation` was added.** Not timestamp, sequence number, session id, or
source — none of them has a demonstrated use yet. Adding fields that look useful
is how a frame becomes a grab-bag.

## FAILURE MODES

- **The cancel-window is narrowed, not closed.** `_cancel_worker()` cancels by
  *task*, not by label:

  ```
  bump ──▶ cancel worker ──▶ flush
                │
          worker dequeues a CURRENT frame here
                │
          cancelled anyway
  ```

  Much narrower than the Phase 7 race — it needs the worker idle at exactly that
  instant — but real. Closing it means checking the in-flight frame's generation
  before cancelling, or re-queueing it if current. Not built: no test demonstrates
  it yet.

- **Partial output survives interruption.** A processor cancelled mid-emit has
  already put stale frames downstream. They now *carry* the old generation, so a
  later flush discards them — but a consumer that already read them is not
  protected. `get_output()` does not check generations.

- **The counter is unbounded.** Irrelevant in practice; worth knowing it is
  monotonic with no reset.

- **No observability.** `interrupt()` returns a dropped-frame count and nothing
  consumes it. Phase 26.

## WHAT WE LEARNED

Some questions cannot be answered by better algorithms, only by better data. The
sweep was not badly written — it was reasoning from information that did not
exist. Adding one integer made the question trivial.

A second lesson, from the test:

> **A passing test is not evidence until you have seen it fail.**

`test_new_turn_frame_survives_interrupt_sweep` was written before the fix and
marked `xfail`, so its failure was verified. But when the marker came off, it
passed for *two* reasons it could not distinguish:

```
labels work                 ← what we meant
nothing is ever dropped     ← also passes
```

With no stale work queued, the flush discarded nothing, so the new frame survived
trivially. Deleting the `bump()` and re-running proved it: the test still passed.
It now queues stale work and asserts it was dropped — and fails when the bump is
removed.

Tests written *after* a fix never get the failure check for free. Break the code
on purpose to earn it.
