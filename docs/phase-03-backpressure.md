# Phase 3 — Queues and backpressure

**Status:** ✅ done
**Code:** `src/rivet/runtime.py` (bounded queues), `src/rivet/pipeline.py`
**Tests:** `tests/test_backpressure.py`

---

## WHY

Phase 2 decoupled producer from consumer with a queue. That raises an obvious
question: **what if the producer is permanently faster than the consumer?**

This is not hypothetical in a voice agent. A microphone produces audio at exactly
realtime, forever, and does not care how long your LLM takes. If the LLM stage runs
at 0.9× realtime, the queue in front of it grows without bound.

With an unbounded queue the failure is slow and then sudden:

```
queue depth:  10 ... 500 ... 40,000 ...
memory:       fine ... fine ... OOM
latency:      120ms ... 8s ... 400s
```

Long before the process dies, it is useless: a reply to something said four
minutes ago is not a conversation. **An unbounded queue converts a throughput
problem into a latency problem and then a memory problem**, and hides it until
everything is on fire.

## WHAT

**Bound every queue.** When a bounded queue is full, `await queue.put()` suspends
the producer until space is available.

```python
self.input_queue: asyncio.Queue[Frame] = asyncio.Queue(maxsize=max_queue_size)
```

That suspension *is* backpressure: the consumer's slowness propagates backwards
and throttles the producer.

```
producer faster than consumer
        ↓
queue fills
        ↓
queue hits maxsize
        ↓
await put() suspends the producer
        ↓
producer now runs at the consumer's rate
```

## HOW

The experiment: `max_queue_size=2`, a processor that takes 1s/frame, 10 frames
pushed as fast as possible.

```
push 1  →  returns instantly   (worker takes it immediately)
push 2  →  returns instantly   (queue: 1)
push 3  →  returns instantly   (queue: 2 — full)
push 4  →  BLOCKS ~1s          ← backpressure engages
push 5  →  BLOCKS ~1s
...                             producer now paced by the consumer
```

### The distinction that trips everyone up

> **"the queue is empty" ≠ "the processor is idle"**

The worker does `get()` — removing the frame from the queue — and *then* spends a
second processing it. During that second the queue reads as empty while the stage
is fully busy.

Queue depth measures **waiting** work, not **in-flight** work. Any observability
built on queue depth alone (Phase 26) will undercount by exactly one frame per
stage, and a "0 depth" alarm threshold will look healthy while every stage is
saturated.

### `task_done()` and `join()`

```python
finally:
    self.input_queue.task_done()
```

`asyncio.Queue` keeps an unfinished-task counter: `put()` increments, `task_done()`
decrements, and `join()` waits for zero. The `try/finally` guarantees the counter
is decremented even if processing is cancelled — otherwise `join()` would hang
forever, which matters immediately in Phase 4.

## TRADE-OFFS

**Bounding trades throughput for predictability.** You will drop or delay work
under sustained overload. That is the correct trade for realtime: bounded latency
and bounded memory beat "eventually processes everything."

**`maxsize` is a latency budget in disguise.** A queue of 100 frames in front of a
1s/frame stage is a 100-second worst-case wait. Queue size should be derived from
the latency you are willing to tolerate, not picked because it looks roomy. The
current default of 100 is a placeholder, not a considered value.

**Backpressure is transitive, and that is the point.** Slow TTS → TTS input fills →
LLM's `put()` blocks → LLM stops consuming → STT's `put()` blocks → the transport
stops reading. The pressure reaches the edge of the system, where something can
actually be done about it.

**But backpressure is the wrong tool for the microphone.** You cannot tell a human
to speak more slowly. At the true edge, the honest options are drop frames, degrade
quality, or interrupt — which is what Phases 6–7 are for. Backpressure protects
internal stages from each other; it cannot protect you from realtime itself.

## FAILURE MODES

- **The terminal `Pipeline.output_queue` is unbounded** (`pipeline.py:22`), the one
  gap in the chain. A consumer that never calls `get_output()` grows it forever.
  Fixed in Phase 7.
- **Deadlock risk if a cycle is ever introduced.** A → B → A with both queues full
  is unrecoverable. The pipeline is linear today, so it cannot happen; Phase 11
  (routing) must take this seriously.
- **A blocked `put()` is invisible.** A stage suspended on a full downstream queue
  looks identical to an idle one from outside. Needs Phase 26.
- **`tests/test_backpressure.py` currently asserts nothing.** It prints timings and
  passes if nothing raises. It documents the behaviour rather than proving it —
  flagged for rework.

## WHAT WE LEARNED

An unbounded queue is not a safety margin, it is a deferred outage. Bounding the
queue converts silent unbounded latency growth into immediate, visible, local
throttling — a much better failure.

And: **queue occupancy and processor busyness are different measurements.**
Conflating them produces monitoring that lies.
