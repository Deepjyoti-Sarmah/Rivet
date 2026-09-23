# Phase 4 — Lifecycle: drain vs stop

**Status:** ✅ done
**Code:** `src/rivet/runtime.py` — `RuntimeState`, `start()`, `drain()`, `stop()`
**Tests:** `tests/test_shutdown.py`

---

## WHY

A worker running `while True` never ends on its own. Shutting down means killing
it — and the naive version is a single `stop()` that cancels the task.

That single verb turns out to conflate two genuinely different intentions:

> **"We're done — finish what you've got."**
> Session ended normally. Frames still queued are the user's last utterance.
> Discarding them loses real work.

> **"Stop now."**
> The user hung up. The connection dropped. Continuing to spend GPU time on a
> reply nobody will hear is pure waste.

One verb cannot serve both. Shipping only "finish everything" means a hung provider
call can block shutdown indefinitely; shipping only "stop now" means normal
shutdown silently discards the user's last sentence.

## WHAT

**Two distinct operations, and an explicit state machine.**

```python
class RuntimeState(Enum):
    CREATED   = "created"
    RUNNING   = "running"
    DRAINING  = "draining"
    STOPPED   = "stopped"
```

```
CREATED ──start()──▶ RUNNING ──drain()──▶ DRAINING ──▶ STOPPED
                        │                                  ▲
                        └──────────── stop() ──────────────┘
```

| | Accepts new work | Finishes queued work | Ends |
| --- | --- | --- | --- |
| `drain()` | ❌ | ✅ | when queue empties |
| `stop()` | ❌ | ❌ | immediately |

**`drain() != stop()`** is the invariant of this phase.

## HOW

### `drain()` — the ordering is the whole trick

```python
self.state = RuntimeState.DRAINING   # 1. close the door FIRST
await self.input_queue.join()        # 2. then wait for the room to empty
await self._cancel_worker()          # 3. then kill the idle worker
self.state = RuntimeState.STOPPED
```

Step 1 must precede step 2. `push()` checks the state and raises once draining, so
no new frames can enter. Without that, a busy producer could keep feeding the queue
and `join()` would never return — drain would hang forever under load.

This is a general pattern: **close the entrance before waiting for the room to
empty.**

`join()` returns when the unfinished counter hits zero, which is why the
`try/finally: task_done()` from Phase 3 is load-bearing here. A missing
`task_done()` turns drain into a permanent hang.

Step 3 still cancels the worker — after `join()` the worker is parked on
`await input_queue.get()`, and the only way out of a `while True` is cancellation.

### `stop()` — set state first, then cancel

```python
self.state = RuntimeState.STOPPED    # no new work, even during the await below
if self.task is not None:
    self.task.cancel()
    try:
        await self.task
    except asyncio.CancelledError:
        pass
```

The `await self.task` is not decoration. `cancel()` only *requests* cancellation;
it schedules `CancelledError` to be raised at the task's next suspension point.
Without awaiting, `stop()` returns while the worker is still running, and cleanup
races with whatever the caller does next.

`stop()` swallows the `CancelledError` **only because it is the one who requested
it** — it is consuming its own signal, not hiding someone else's.

## TRADE-OFFS

**`drain()` has no timeout.** A processor that hangs forever hangs drain forever.
Acceptable now; a real system needs `drain(timeout=…)` falling back to `stop()`.
Phase 8.

**Shutdown is one-way.** No `STOPPED → RUNNING`. A stopped runtime is dead, and
`start()` raises from any state but `CREATED`. This simplicity is what makes
interruption (Phase 6) a genuinely *separate* concept rather than a stop/start
cycle.

**State is a plain attribute with no lock.** Every check-then-act on it is a race
waiting to happen — `interrupt()` checks state, then awaits, and by the time it
resumes the state may have changed. Real bug, identified and fixed in Phase 7.

**Pipeline-level lifecycle is incomplete.** `Pipeline` has `stop()` but no
`drain()`, and `stop()` is not idempotent.

## FAILURE MODES

- **`stop()` from `CREATED`** sets `STOPPED` with `task is None`, after which
  `start()` raises. A runtime can be killed before it ever runs.
- **`drain()` from any state but `RUNNING` silently returns.** Calling `drain()` on
  an already-draining runtime does nothing and doesn't wait — the caller believes
  it drained when it didn't.
- **Concurrent `stop()` + `drain()` is unguarded.** Both mutate state across await
  points.
- **Nothing distinguishes "stopped cleanly" from "stopped because it crashed."**
  Needs `FAILED`. Phase 7 adds it; Phase 8 builds the full machine.
- **`tests/test_shutdown.py` uses `await asyncio.sleep(0)`** to let a state
  transition land — a single event-loop tick. It encodes an implicit assumption
  that `drain()` reaches `DRAINING` within exactly one yield. Flaky by
  construction; should be an `Event`.

## WHAT WE LEARNED

"Shutdown" is not one operation. Separating *graceful* from *forced* is the first
real lifecycle decision, and the ordering inside `drain()` — close the door, then
wait — is the kind of detail that only shows up under load, when a producer is
still pushing.

Phase 5 examines what `cancel()` actually does, because "stop immediately" turns
out to be far less immediate than it sounds.
