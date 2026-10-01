# Phase 9 — Frame metadata (generation ids)

**Status:** ✅ done
**Code:** `src/rivet/frames.py`, `src/rivet/runtime.py`, `src/rivet/pipeline.py`
**Tests:** `tests/test_cancellation_propagation.py`
**Closes:** the race left open by [phase 7](phase-07-cancellation-propagation.md),
and the cancel-window and standalone-stamping holes this document left open
afterwards

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

Owning the counter also means owning the stamp, because `Pipeline.push` is not in
the path at all:

```python
if self._owns_generation:
    frame.generation = self.generation.value
```

Without it a standalone runtime breaks on its first interrupt. It owns the counter,
so it moves the counter, and every frame it is later handed still carries the
default `0`:

```
interrupt()          bump → value 1
push(TextFrame(...)) generation stays 0
_run()               is_stale(0) → 0 < 1 → skip
```

No error, no log, no work. This surfaced as a test failure rather than a report:
`test_interrupt_keeps_runtime_running` asserted the state string and then stopped,
so it never fed the runtime anything again.

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

### A frame already dequeued

The sweep can only see queues. A frame a worker has pulled off one is in neither
place, so cancelling that worker drops the frame with no trace: the `finally`
calls `task_done()`, the queue looks empty, and `drain()` reports success.

The runtime records what it is holding, and the interrupt reads that before it
cancels anything:

```python
self._inflight = frame          # in _run, set after the stale check
...
if not self._holds_current_work():
    await self._cancel_worker()
```

A current frame survives its worker. A stale one is cancelled exactly as before,
and the sweep still discards everything queued behind it.

Set after the stale check rather than before: a frame that was correctly dropped
as stale is not in flight, and resurrecting it would undo the sweep.

**There is no natural race in this path today.** Between the pipeline's `bump()`
and a runtime's cancel decision there is no `await` — an uncontended
`asyncio.Lock.acquire` returns without suspending, and awaiting a coroutine does
not hand control to the loop — so no push can land a current frame in the gap.
`test_current_frame_survives_await_between_bump_and_cancel` therefore installs the
missing `await` itself, by parking the stage's `interrupt()` on an `Event`, and
pins the invariant:

```
a frame stamped with the current generation
is never discarded by an interrupt
```

Phase 10 rewrites this loop. One `await` added there is all it takes to make the
window live, which is the reason for the test rather than a comment.

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

- **`interrupt()` is no longer a hard stop on current work.** Closing the window
  traded a silent frame loss for a worker that can outlive the call:

  ```
  interrupt() returns
      │
      ├── stage A: still running, mid-LLM-call   ← by design
      └── stage B: cancelled
  ```

  Stages are non-uniform within a single interrupt. A processor parked on a slow
  call emits *after* the sweep, carrying the current generation, so its output is
  not stale work — but nothing orders that output against the turn that followed
  it. `stop()` is unaffected: `_shutdown()` cancels unconditionally.

- **Partial output survives interruption.** A processor cancelled mid-emit has
  already put stale frames downstream. They now *carry* the old generation, so a
  later flush discards them — but a consumer that already read them is not
  protected. `get_output()` does not check generations.

- **Two places can stamp, and one of them is easy to get wrong.**
  `Pipeline.push` stamps because the pipeline owns the counter. A runtime that
  owns its counter stamps in `push` as well, and it must do so *before* the put:

  ```
  push(TextFrame)  →  stamp now        ✓  turn it was pushed in
                  →  await queue.put
                  →  stamp later       ✗  turn the sweep that freed the slot
  ```

  A push blocked on a full queue is released by the sweep of the very interrupt
  that supersedes it. Stamping after the wait admits the frame to the turn it was
  pushed in front of. Two tests pin this: one that a standalone runtime still
  works after an interrupt, one that a push which waited is not admitted.

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

A third lesson, from a test that had to manufacture its own race:

> **A test can only prove what its seam can reach.**

The window has no `await` in it, so no schedule can enter it and the test has to
install the missing suspension itself. Two attempts failed instructively. Parking
inside `_cancel_worker` sat *after* the cancel decision, so it could only delay a
cancel that had already been chosen — and the frame under test was never offered
to the decision at all. Patching `Generation.bump` with an `async def` produced a
coroutine that nobody awaited, because `bump()` is synchronous, and the test went
green for a garbage reason.

A test that passes when you expected it to fail is reporting on the seam before it
reports on the code.
