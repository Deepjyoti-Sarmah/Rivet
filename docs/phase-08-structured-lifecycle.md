# Phase 8 — Structured pipeline lifecycle

**Status:** ✅ done
**Code:** `src/rivet/runtime.py`, `src/rivet/pipeline.py`
**Tests:** `tests/test_lifecycle.py`
**Builds on:** [phase 4](phase-04-lifecycle.md), which introduced `drain` vs `stop`

---

## WHY

Phase 4 established the two verbs. It left three things unfinished, and each is a
production outage waiting to happen.

### 1. The state lied

```python
self.state = STOPPED          # promise made
await self._cancel_worker()   # worker still alive through all of this
```

`state` is a **promise to an observer**:

```
state == RUNNING    you may push
state == STOPPED    the worker is gone
```

For the entire duration of that `await`, the promise was false.

```
t0  state = STOPPED
t1  observer: "stopped — safe to tear down the queue"
t2  worker:   still writing to that queue
```

### 2. The wait was unbounded

```python
await self.input_queue.join()    # returns when the queue empties
```

That is a promise depending on code you do not control:

```
processor is quick        → join() returns
processor is slow         → join() returns eventually
processor never returns   → join() NEVER returns
```

A hung provider call, a socket with no timeout, an infinite loop — and shutdown
hangs forever. The process will not exit; the container will not restart. Only
`SIGKILL` ends it.

### 3. Shutdown had no result

Even with a timeout added, `drain()` returning after giving up is
indistinguishable from `drain()` returning after finishing. The caller believes
shutdown succeeded either way.

```
queue emptied, everything finished     ← clean
timed out, force-stopped, work lost    ← not clean
```

For a voice agent, "session ended" and "session ended, 3 utterances dropped" are
different events.

## WHAT

```
CREATED ──start()──▶ RUNNING ──drain()──▶ DRAINING ──▶ STOPPING ──▶ STOPPED
                        │                                 ▲
                        └────────── stop() ───────────────┘
                        │
                        └──── processor raises ────▶ FAILED
```

| Added | |
| --- | --- |
| `STOPPING` | true while the worker unwinds |
| `drain(timeout=…)` | bounded wait |
| `drain() -> bool` | emptied, or gave up |
| `Pipeline.drain()` | source → sink |
| `_shutdown_lock` | concurrent `stop()` serialised |

## HOW

### Every state name is true while it is set

```python
self.state = RuntimeState.STOPPING
await self._cancel_worker()
self.state = RuntimeState.STOPPED
```

`STOPPING` means *in progress, not finished*. `STOPPED` is now a guarantee rather
than an intention.

`push()` already rejects anything that is not `RUNNING`, so the new state needs no
special handling there.

### The wait is bounded, and reports what happened

```python
        drained = True

        try:
            if timeout is None:
                await self.input_queue.join()
            else:
                await asyncio.wait_for(self.input_queue.join(), timeout=timeout)
        except TimeoutError:
            drained = False
```

`timeout=None` keeps the old behaviour for callers that genuinely want to wait.
The bool is not decoration — the two outcomes have different consequences.

### The lock is released across the wait

```python
async with lock:
    state = DRAINING      # short
await join()              # LONG — no lock held
async with lock:
    cancel worker         # short
    state = STOPPED
```

Holding the lock across `join()` would let a hung processor block every other
lifecycle call — including the `stop()` meant to rescue it.

> **Hold a lock for state transitions, never across an unbounded wait.**

### First caller owns the shutdown

Releasing the lock means `drain()` can be overtaken. It re-checks on the way out:

```python
async with self._lifecycle_lock:
    if self.state != RuntimeState.DRAINING:
        # A concurrent stop() got here first and owns the shutdown.
        return drained
```

Shutdown is reached from several uncoordinated paths **by design**:

```
normal exit path  ──┐
signal handler    ──┼──▶ stop()
error handler     ──┤
test teardown     ──┘
```

They do not know about each other. Overlapping calls must be no-ops, not crashes
and not double-cancels.

> **Whoever gets there first owns the shutdown. Everyone else observes.**

### Close the entrance before waiting

```python
self.state = RuntimeState.DRAINING   # push() raises from here
await self.input_queue.join()        # now the queue can actually empty
```

Reverse the order and a busy producer keeps refilling the queue while you wait for
it to empty. `join()` never returns — not because anything is broken, but because
you are waiting on a condition someone else keeps undoing.

### Pipeline drains source → sink

```python
for runtime in self.runtimes:
    if not await runtime.drain(timeout=timeout):
        drained = False
```

Same direction as the interrupt sweep, for the opposite reason. Interrupt goes
source-first to stop the tap; drain goes source-first so each stage finishes
*feeding* the next before that one is drained. Drain sink-first and stage C shuts
down while A and B still have work to hand it.

`drained` is anded across stages: one stage dropping work means the drain was not
clean, even if the others finished.

## TRADE-OFFS

**Timeout is per stage, not total.** Worst case is `timeout × len(runtimes)`. A
total budget would need to be divided or tracked across stages; per-stage is
simpler and easier to reason about. Documented on the method rather than hidden.

**`FAILED` is terminal and not overwritten.** `stop()` on a failed runtime is a
no-op, so `runtime.error` survives shutdown. Why it died matters after it has
stopped.

**No `Pipeline.state`.** A pipeline-level state would have to aggregate six states
across N runtimes, and no caller needs it yet. Per-runtime state is inspectable.
Adding it now would be an abstraction with no demonstrated use.

**Still no restart.** `STOPPED` is terminal; `start()` raises from anything but
`CREATED`. This is what keeps `interrupt()` a genuinely separate concept rather
than a stop/start cycle.

## FAILURE MODES

- **A processor that ignores cancellation defeats the timeout too.** `drain`
  bounds the *wait*, then calls `_cancel_worker()` — which awaits the task. A
  coroutine with no `await` in it, or one that catches `CancelledError` without
  re-raising, hangs there instead. The bound moved the hang; it did not remove it.
  Genuinely fixing this needs the worker in a separate thread or process.
- **Drain timeout leaves the queue non-empty.** Force-stopped work is dropped
  silently beyond the returned bool — no per-frame record of what was lost.
- **No `drain()` on a `FAILED` pipeline.** It returns `False` per stage because
  the state is not `RUNNING`, which reads as "timed out". The two are different
  and the bool cannot express it.
- **No signal handling.** Nothing wires `SIGTERM` to `drain()`. That belongs with
  the transport layer (Phase 15).

## WHAT WE LEARNED

```
a state name must be true the whole time it is set
any wait on foreign code needs a bound
losing work and finishing work are different results
first caller owns the shutdown, the rest observe
never hold a lock across an unbounded wait
close the entrance before waiting for the room to empty
```

### On the tests

Both changes were mutation-checked — the discipline adopted after Phase 9, where a
test passed for two reasons it could not distinguish.

```
STOPPING removed       → test_state_is_stopping fails
timeout bound removed  → exit 124, the suite HANGS
```

The second is the one worth noting. `test_drain_times_out_on_a_stuck_processor`
does not merely assert a `False` return — with the bound removed it **reproduces
the production failure exactly**: shutdown never completes and the only way out is
to kill the process.

> A test for a hang has to be able to hang.
