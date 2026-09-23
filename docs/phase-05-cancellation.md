# Phase 5 — Cancellation

**Status:** ✅ done
**Code:** `src/rivet/runtime.py`
**Tests:** `tests/test_cancellation.py`

---

## WHY

Phase 4's `stop()` calls `task.cancel()`. It is worth being precise about what
that actually does, because the intuition most people bring — "kill the task" — is
wrong in a way that causes real bugs.

## WHAT

**`asyncio` cancellation is cooperative, not pre-emptive.**

`task.cancel()` does not terminate anything. It sets a flag requesting that
`asyncio.CancelledError` be raised inside the coroutine **at its next suspension
point** — the next `await` that actually yields to the event loop.

```python
async def process(self, frame):
    await asyncio.sleep(100)   # ← cancellation lands HERE
```

versus:

```python
async def process(self, frame):
    for _ in range(10_000_000):
        compute()              # ← no await; cancellation cannot land
    return result              # runs to completion regardless
```

A coroutine with no `await` in it **cannot be cancelled**. `cancel()` will be
recorded and delivered only when it next suspends — and if it never suspends, never.

There is a single event loop thread. A coroutine that doesn't yield doesn't just
resist cancellation — it stalls *every other task in the process*.

## HOW

### The pattern

```python
try:
    await something()
except asyncio.CancelledError:
    cleanup()
    raise          # ← mandatory
```

**Never swallow `CancelledError`.** It is not an error in the usual sense; it is a
control signal travelling up the stack, and something above you is `await`ing your
task expecting it to end. Catching it without re-raising means:

- the canceller's `await task` never completes — shutdown hangs
- the task keeps running, "uncancellable"
- in 3.11+, a swallowed cancel can corrupt enclosing `TaskGroup` / `timeout` state

The one legitimate exception is the code that *requested* the cancellation
consuming its own signal — which is exactly what `stop()` does:

```python
self.task.cancel()
try:
    await self.task
except asyncio.CancelledError:
    pass           # I asked for this; it's mine to absorb
```

### `cancel()` is a request; `await` is the confirmation

```python
task.cancel()       # request sent, returns immediately — task still running
await task          # NOW it has actually finished unwinding
```

Skipping the `await` means proceeding while the worker is still alive and may
still be writing to queues you are about to inspect.

### Cancellation during cleanup

`_run` wraps processing in `try/finally`:

```python
finally:
    self.input_queue.task_done()
```

`finally` blocks **do** run when `CancelledError` passes through. This keeps the
queue's unfinished counter accurate even for a frame that was killed mid-flight —
without it, a single cancellation would leave `join()` hanging forever, breaking
`drain()`.

### Where cancellation lands in `_run`

Three distinct suspension points, with different consequences:

```python
frame = await self.input_queue.get()        # (1) idle — cleanest
output = await self.processor.process(...)  # (2) mid-work — frame is lost
await self.output_queue.put(output)         # (3) mid-emit — PARTIAL OUTPUT
```

Point (3) is the nasty one. A processor that returned three frames may have
emitted only the first before being cancelled. Downstream receives a truncated
result with no indication it is incomplete — half a sentence handed to TTS.

Note also that at point (1) the `try` block was never entered, so no spurious
`task_done()` is called. The accounting stays correct by construction.

## TRADE-OFFS

**Cooperative cancellation is the price of single-threaded async.** No locks, no
data races, no pre-emption at arbitrary points — but you get cancellation only
where you yield. CPU-bound work must be pushed to a thread or process pool
(`asyncio.to_thread`) or it will freeze the loop.

**Cancellation granularity is the task.** We can kill "the worker," not "this one
frame." A finer unit would need per-frame tasks — losing the ordering guarantee
from Phase 2. Not worth it.

**Partial output is accepted, not solved.** Nothing currently marks a truncated
emission. Making it visible needs frame metadata (Phase 9); handling it properly
needs the error model (Phase 13).

## FAILURE MODES

- **Uncancellable processors.** A provider SDK doing blocking I/O, or a tight
  compute loop, ignores cancellation entirely and blocks the whole loop.
- **Shields.** `asyncio.shield()` protects a coroutine from cancellation — correct
  for "must finish" cleanup, silently fatal to interruption if misapplied.
- **Swallowed cancellation** turns a clean shutdown into a hang. The single most
  common asyncio bug.
- **Cancelling a completed task is a no-op** that returns `False`. Code assuming
  `cancel()` always "worked" will be wrong.
- **Cancelling the worker does nothing to queued frames.** They sit there
  untouched, ready to be picked up by whatever runs next. This is the observation
  that Phase 6 is built on.

## WHAT WE LEARNED

`task.cancel()` requests; it does not kill. Delivery happens at an `await`, which
means the shape of your code determines whether cancellation is even possible.

And the observation that drives everything after this:

> **Cancelling the worker affects the frame being processed. It does nothing to
> the frames waiting in the queue.**

"Cancel current work" and "discard stale queued work" are two separate operations.
Phase 6 needs both.
