# Phase 6 — Interruption

**Status:** ✅ done (single stage)
**Code:** `src/rivet/runtime.py` — `interrupt()`
**Tests:** `tests/test_interrupt.py`

---

## WHY

The defining behaviour of a voice agent: the user starts talking while the agent
is mid-answer. Everything the agent is currently working on is now worthless — but
**the agent must keep running** to handle what the user is about to say.

Neither Phase 4 verb fits:

| | Why it's wrong |
| --- | --- |
| `drain()` | finishes the dead reply — the bot talks over the user |
| `stop()` | throws away the runtime — nothing left to handle the new turn |

Interruption is a third thing: **discard the work, keep the worker.**

```
stop()       → runtime ends
drain()      → finish existing work, then runtime ends
interrupt()  → discard current + stale work, runtime stays alive
```

## WHAT

```python
async def interrupt(self) -> None:
    if self.state != RuntimeState.RUNNING:
        return

    await self._cancel_worker()      # 1. kill the frame in flight
    self._flush_input()              # 2. discard the frames waiting
    self.task = asyncio.create_task(self._run())   # 3. fresh worker
```

Three steps, and **step 2 exists only because of the Phase 5 finding**: cancelling
the worker does nothing to the queue. Without it, the new worker would immediately
pick up the previous turn's backlog and carry on producing the dead answer.

```
BEFORE                          AFTER interrupt()
  worker: processing A            worker: fresh, idle
  queue:  [B, C, D]               queue:  []
  state:  RUNNING                 state:  RUNNING  ← unchanged
```

State stays `RUNNING` throughout. That is the point.

## HOW

### Cancel and flush are separate operations

```python
while not self.input_queue.empty():
    self.input_queue.get_nowait()
    self.input_queue.task_done()
```

`get_nowait()` removes without suspending; the paired `task_done()` keeps the
unfinished counter honest so a later `drain()` can still `join()`. Dropping that
pairing would make drain hang forever after any interrupt — a bug that would
surface hours later, in an unrelated code path.

### Why a new task rather than a resumable loop

Once `CancelledError` propagates out of `_run`, that coroutine is finished — a
cancelled task cannot be restarted. Rebuilding the worker is the only option. It is
cheap: an `asyncio.Task` is a small object, not a thread.

### Control is not data

An early temptation is to signal interruption in-band:

```python
await runtime.push(TextFrame("STOP"))     # ❌
```

This is wrong on three counts:

1. **Ambiguity.** A user who says "stop" produces a transcript frame with that
   exact text. Data and command become indistinguishable.
2. **It queues.** The whole purpose is to skip the backlog. A control signal that
   waits behind five seconds of audio has already failed.
3. **Every processor must now know about it**, re-coupling business logic to
   orchestration.

So interruption is an **out-of-band method call**, not a frame. `InterruptFrame`
exists in `frames.py` for a future explicit control plane (Phase 16), but nothing
routes on it today.

The general rule:

> Data flows *through* the pipeline. Control acts *on* the pipeline.

## TRADE-OFFS

**Discarded work is discarded permanently.** No buffering, no replay. Correct for
voice — a superseded sentence has no value — but it means an over-eager
interruption (a cough misread as speech by VAD) destroys a good answer with no
recovery. Phase 22–24 must care about false positives.

**Partial output escapes.** If the worker was cancelled at `await output_queue.put()`
having emitted 1 of 3 frames, that one frame is already downstream and `interrupt()`
does not chase it. Stale output survives. This is the seed of Phase 7.

**Interrupt is per-runtime and has no lock.** The state check at the top is
followed by an `await`, so a concurrent `stop()` landing mid-interrupt can be
overwritten by step 3 — **resurrecting a stopped runtime**. Real race, fixed in
Phase 7.

## FAILURE MODES

- **Interrupt does not propagate.** It affects exactly one stage. In an A→B→C
  chain, interrupting A leaves B still generating and C's queue still full. This is
  the entire subject of Phase 7.
- **Duplicate interrupts race.** Two concurrent calls can cancel each other's
  freshly-created worker, leaving zero or two workers.
- **In-flight frames between stages are invisible.** `Pipeline`'s forwarder task
  can hold a frame that is in neither queue, so no flush can reach it.
- **No way to tell interrupted from completed.** Nothing records that a turn was
  abandoned — no metric, no log, no marker on the frames.
- **Tests leak tasks.** `tests/test_interrupt.py` never calls `stop()`, so each
  test leaves a live worker behind.

## WHAT WE LEARNED

Interruption is a distinct lifecycle operation, not a variant of shutdown — *keep
the machine, drop the work*. It needs both halves of the Phase 5 finding: cancel
the in-flight frame **and** flush the queued ones.

And the limitation that defines what comes next:

> A per-stage interrupt is not enough. Stale work does not live in one stage — it
> is smeared across the whole chain, in workers, in queues, and in the gaps
> between them.

Phase 7 makes an interrupt reach the entire pipeline.
