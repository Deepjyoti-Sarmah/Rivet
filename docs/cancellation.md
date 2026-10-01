# Cancellation, interruption, and stale work

Cross-cutting reference for Phases 5–7. Read
[phase-05](phase-05-cancellation.md), [phase-06](phase-06-interruption.md) and
[phase-07](phase-07-cancellation-propagation.md) for how each piece arrived.

---

## WHY

A voice agent spends most of its time producing work that may become worthless
before it is delivered.

```
user:  "What's the weather in Paris?"
agent:  [STT done] [LLM generating] [TTS has 3 sentences queued]
user:  "No wait — Tokyo!"
```

Everything about Paris is now garbage. A batch system can ignore this. A realtime
one cannot: finishing dead work means talking over the user.

So the question every phase from 5 onward answers is:

> **When work becomes worthless mid-flight, how do you get rid of it?**

---

## WHAT: three categories, three actions

Stale work hides in three places. They do not want the same treatment.

| Where it is | Action | Why |
| --- | --- | --- |
| A worker is running it now | **cancel** | something is executing |
| Sitting in an inbox | **discard** | nothing is executing — just delete |
| A frame from the **new** turn | **leave alone** | it is the thing the user wants |

Conflating these is the central trap.

```
cancel   ≠   discard   ≠   leave alone
```

"Just cancel everything downstream" gets the first two right and the third
catastrophically wrong. The user experience of that failure is not a bot that
talks too long — it is a bot that goes **silent**, having heard the new question
and then killed its own work.

---

## HOW

### Cancellation is cooperative

`task.cancel()` does not kill anything. It requests that `CancelledError` be
raised at the coroutine's **next suspension point**.

```python
await asyncio.sleep(100)   # cancellation lands here

for _ in range(10**8):     # no await — cancellation cannot land
    compute()              # runs to completion, and blocks every other task
```

A coroutine with no `await` cannot be cancelled.

### `cancel()` requests, `await` confirms

```python
task.cancel()     # request sent; task still running
await task        # now it has actually unwound
```

Skipping the `await` means continuing while the worker may still be writing to
queues you are about to inspect.

### Never swallow `CancelledError`

```python
try:
    await something()
except asyncio.CancelledError:
    cleanup()
    raise          # mandatory
```

It is a control signal, not an error. Swallowing it hangs whoever is awaiting the
task and corrupts enclosing `TaskGroup` / `timeout` state.

The one exception is code consuming **its own** request:

```python
self.task.cancel()
try:
    await self.task
except asyncio.CancelledError:
    pass           # I asked for this
```

### The three landing sites in the worker loop

```python
frame = await self.input_queue.get()         # (1) idle — clean
outputs = await self.processor.process(...)  # (2) mid-work — frame lost
await self.output_queue.put(output)          # (3) mid-emit — PARTIAL OUTPUT
```

(3) is the ugly one: a processor that returned three frames may have emitted only
one. Downstream gets a truncated result with no marker saying so. Accepted and
documented, not solved — making it visible needs frame metadata (Phase 9).

### Cancelling ≠ clearing

```
cancel worker:   affects the frame being processed
                 does nothing to the queue

BEFORE                  AFTER cancel only
  worker: A               worker: dead
  queue:  [B, C, D]       queue:  [B, C, D]   ← still there
```

A fresh worker would pick up B and carry on producing the dead answer. This is why
`interrupt()` needs both halves.

---

## The three lifecycle verbs

```
stop()       cancel now, runtime ends
drain()      finish queued work, then runtime ends
interrupt()  discard current + queued work, runtime stays alive
```

`interrupt()` is the voice-agent verb: **keep the machine, drop the work.**

```
BEFORE                    AFTER interrupt()
  worker: processing A      worker: fresh, idle
  queue:  [B, C, D]         queue:  []
  state:  RUNNING           state:  RUNNING   ← unchanged
```

### drain(): close the door before waiting

```python
self.state = DRAINING          # 1. no new work accepted
await self.input_queue.join()  # 2. wait for the room to empty
await self._cancel_worker()    # 3. kill the now-idle worker
```

Step 1 must precede step 2, or a busy producer keeps refilling the queue and
`join()` never returns.

---

## Propagation across a chain

One stage clearing itself is not enough.

```
A → B → C

interrupt A only:
  A   cleared
  B   still running the dead turn
  C   inbox still full
```

### Ordering: source → sink

```
sink → source              source → sink
  clear C                    clear A     tap off
  clear B                    clear B     nothing refills
  A still emits ✗            clear C     nothing refills
  B dirty again
```

> **Shut off the tap before mopping.**

### Ownership

```python
for runtime in self.runtimes:
    runtime.task.cancel()      # no
    await runtime.interrupt()  # yes
```

> **A task is cancelled only by the object that created it.**

Each runtime owns its worker and inbox — and knows to also flush and respawn.
`Pipeline` owns only the **ordering**.

### Flush the terminal queue too

```
A → B → C → [output queue] → caller
                   ↑
             stale lives here
```

Frames that already reached the end are still Paris.

### Post-condition

```
when Pipeline.interrupt() returns:
    every runtime is RUNNING
    every input queue is empty
    no worker holds a pre-interrupt frame
    the output queue is empty
```

---

## Races

### Check-then-act

```python
if self.state != RUNNING:   # check
    return
await self._cancel_worker() # ← state can change HERE
self.task = create_task(…)  # act — may resurrect a stopped runtime
```

Fix: a lifecycle lock, and **re-check state after acquiring it**. The first check
is stale by the time you hold the lock.

`drain()` releases the lock while joining, so a hung processor cannot block a
force-stop.

### Duplicate interrupts

Barge-in detection fires twice; sweep 2 cancels the worker sweep 1 just spawned.
Serialised by the same lock.

### Invisible failure

A processor exception killed the worker while `state` still said `RUNNING`, and
`push()` kept accepting frames nobody would process. Now sets `FAILED` and records
the exception. A real error model is Phase 13.

---

## TRADE-OFFS

**Discarded work is gone.** No buffering, no replay. Correct for voice — a
superseded sentence has no value — but a false interruption (a cough misread by
VAD) destroys a good answer irrecoverably. Phases 22–24 must care about false
positives.

**Interrupt is out-of-band, not a frame.** The in-band alternative:

```python
await runtime.push(InterruptFrame())
```

fails three ways: a user saying "stop" is indistinguishable from the command; the
push is subject to backpressure, so control waits behind data —

```
queue: [audio audio audio audio audio]
                                      ↑ InterruptFrame waits here
```

— which is exactly what interruption exists to avoid; and every processor would
have to know about it. `InterruptFrame` exists in `frames.py` for a future
priority control path (Phase 16); nothing routes on it today.

> **Data flows *through* the pipeline. Control acts *on* the pipeline.**

**Cancellation granularity is the task, not the frame.** Per-frame tasks would
lose the ordering guarantee that one worker per stage buys for free.

---

## FAILURE MODES

- **The sweep cannot protect a new turn.** See below — the open one.
- **Partial output escapes.** A frame emitted before cancellation is already
  downstream; the sweep flushes queues but cannot recall what a consumer has read.
- **Uncancellable processors.** Blocking provider I/O or a tight compute loop
  ignores cancellation and freezes the loop.
- **`asyncio.shield()`** silently makes work immune to interruption.
- **No interruption metrics.** Nothing records that a turn was abandoned.
  `interrupt()` returns a dropped-frame count; nothing consumes it yet (Phase 26).

---

## The problem Phase 9 solved

Up to Phase 7 the sweep decided what was stale **by timing**: anything in an inbox
when cleanup ran was assumed old. That assumption is wrong for anything that
arrived a moment ago.

```
sweep:   cancel A ──────────── flush A's inbox
                    │
new:          push "Tokyo" ──▶ lands in the inbox
                                        │
                                   deleted
```

Reordering moves the window without closing it:

| Order | What breaks |
| --- | --- |
| cancel → flush | new arrival lands between them |
| flush → cancel | the dying worker emits into the flushed inbox |
| lock out `push` | the new turn is rejected instead of lost |

The window exists because, looking at a frame in an inbox:

```
TextFrame("...")   Paris?
TextFrame("...")   Tokyo?
```

**nothing on the frame says which turn it belongs to.** The information required
does not exist.

[Phase 9](phase-09-frame-metadata.md) added it:

```
"Paris"  →  generation 10
"Tokyo"  →  generation 11

current generation is 11
  frame says 10  →  discard
  frame says 11  →  keep
```

Timing and ordering stop mattering, because the answer is written on the frame.

> **cancel-by-timing → invalidate-by-label**

`Pipeline` bumps the counter **before** sweeping, so everything already in the
system is stale by definition and anything pushed afterwards carries the new
number. Flushing became a filter — drain, keep the current, re-queue in order —
rather than an unconditional delete.

### What remains

The window is closed. `_cancel_worker()` used to cancel by **task**, not by label:

```
bump ──▶ cancel worker ──▶ flush
              │
        worker dequeues a CURRENT frame here
              │
        cancelled anyway
```

A dequeued frame is in neither queue, so the flush could never see it. The runtime
now records the frame it holds and skips the cancel when that frame is current:

```python
if not self._holds_current_work():
    await self._cancel_worker()
```

`interrupt()` therefore no longer guarantees that in-flight work stopped. A
processor parked on a slow call outlives the call and emits after the sweep. That
output carries the current generation, so it is not stale work, but nothing orders
it against the turn that followed. `stop()` still cancels unconditionally.

There is no natural race in the path today: between the pipeline's `bump()` and a
runtime's cancel decision there is no `await`, so no push can land in the gap.
Phase 10 rewrites that loop, and
`test_current_frame_survives_await_between_bump_and_cancel` installs the missing
`await` to keep the invariant pinned.

Also: a consumer that has already read a stale frame via `get_output()` is not
protected. Flushing reaches queues, not readers.
