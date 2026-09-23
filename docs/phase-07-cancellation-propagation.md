# Phase 7 — Cancellation Propagation

**Status:** 🔨 in progress
**Prerequisites:** Phases 1–6 (frames, runtimes, backpressure, lifecycle, cancellation, single-stage interrupt)
**Leads into:** Phase 9 (frame metadata / generation ids)

> Guided exercise. **You write every line of implementation code.** The assistant
> poses problems, asks questions, states invariants, and reviews — see
> [`AGENTS.md`](../AGENTS.md).

---

## The problem

Rivet can interrupt **one** stage today. `ProcessorRuntime.interrupt()` cancels
that stage's worker, empties its own input queue, and restarts the worker. That
works, and it's genuinely useful.

But a voice agent is a chain:

```
A (STT)  →  B (LLM)  →  C (TTS)
```

When the user barges in mid-answer, every frame in that chain belongs to the dead
turn. Interrupt A alone and B keeps generating the old reply while C's inbox is
still full of old text to speak.

**The agent keeps talking after being interrupted.** That is the bug this phase
closes.

`tests/test_cancellation_propagation.py` is named for this behavior but currently
only asserts `processor_a.cancelled.is_set()` — B and C never receive a frame, so
nothing downstream is exercised.

---

## Ground rules

- **No step's code is written before its test.** Test first, watch it fail for the
  right reason, then implement.
- No `asyncio.sleep()` for synchronization — use `asyncio.Event` or `queue.join()`.
- Always `except asyncio.CancelledError: ... raise`. Never swallow it.
- Every test proves a behavior; none merely executes code.

🔍 marks a checkpoint — hand over the diff for review.

---

## Step 0 — Make the ground solid ✅

Small real defects that would otherwise waste time later.

- [x] **0.1** `uv run python -m rivet.main` crashed with
      `RuntimeError: Cannot push frame while runtime is RuntimeState.CREATED` —
      the demo never called `await pipeline.start()`, and `start()` is the only
      transition out of `CREATED`. Behind that crash sat a second bug:
      `result = await pipeline.push(...)` treats `push()` as returning the
      processed frame, but it returns `None`. Fixed by calling `start()`, then
      `push()`, then `get_output()`, then `stop()`.
- [x] **0.2** `src/rivet/processors/debug.py` imported `from src.rivet...` while
      every other module uses `from rivet...`. **This was not a latent problem —
      it was live.** See below.
- [x] **0.3** `InturuptFrame` → `InterruptFrame`; leftover `# breakpoint()`
      removed from `AudioFrame`.
- [x] **0.4** Added `[tool.pytest.ini_options]` with `asyncio_mode = "strict"` and
      `asyncio_default_fixture_loop_scope = "function"`. Moved `pytest` out of
      `[project] dependencies` (a test framework is not something a *consumer* of
      the library needs installed) and into the dev group with `pytest-asyncio`.
- [x] **0.5** Added `.gitignore`.

### What 0.2 actually turned out to be

The expectation was a `ModuleNotFoundError`, since there is no `src/__init__.py`.
Instead the import **succeeded** — Python 3.3+ implicit namespace packages make
`src.rivet.processor` importable whenever the project root is on `sys.path`.

Which means both module paths loaded, as two separate module objects:

```python
>>> from rivet.processor import Processor as A
>>> from src.rivet.processor import Processor as B
>>> A is B
False
>>> issubclass(DebugProcessor, A)     # the Processor Pipeline knows about
False
>>> issubclass(DebugProcessor, B)     # a different class, same source file
True
```

**`DebugProcessor` was not a `rivet.Processor` at all.** The ABC was defined
twice, the class registered against the wrong copy, and it worked only because
nothing in the codebase does an `isinstance` check yet.

This is strictly worse than a crash. A crash is immediate and names its cause. This
fails silently now and surfaces much later — the first time anything type-checks a
processor, dispatches on frame class, or catches a specific exception type — as
"this object is obviously an X, why does `isinstance` say it isn't?" The same
mechanism duplicates module-level state: two `Processor` registries, and if the
module had a cache or a counter, two of those too.

**Rule:** one import path per module. A `src/` layout means the package is `rivet`,
never `src.rivet`.

🔍 **Checkpoint 0** — complete. `uv run python -m rivet.main` prints
`TextFrame(..., text='HELLO RIVET!')`; all 10 existing tests still pass.

---

## Step 1 — Prove the bug exists

Before fixing anything, **write a failing test that demonstrates the agent keeps
talking.**

Build a 3-stage chain. Get a frame into B so B is mid-process. Interrupt A. Assert
what you *want* to be true: B was cancelled, C's inbox is empty.

Watch it fail. That failure is the justification for this entire phase.

Answer in your own words **before** writing it:

- How do you deterministically get a frame "into B and mid-process" without
  `asyncio.sleep()`? (`BlockingProcessor` already has the tool.)
- Why must A *finish and emit* before you interrupt, for this test to mean anything?
- `BlockingProcessor` is currently duplicated across two test files, plus a dead
  byte-for-byte clone. Where should it live instead?

🔍 **Checkpoint 1** — the failing test. Failing for the *right reason* is the part
people get wrong.

---

## Step 2 — The three categories

No code. Answer in writing.

When you clear the chain, stale work hides in three places. For each, what is the
correct action?

| Where the stale work is | What do you do? |
| --- | --- |
| A worker is running it right now | ? |
| Sitting in an inbox, not started | ? |
| A frame from the **new** turn | ? |

Then the question this whole phase turns on:

> "Just cancel every task downstream" gets two of these right and one
> **catastrophically wrong**. Which one — and what does the *user* experience when
> it goes wrong?

🔍 **Checkpoint 2** — your answers. Get the third row and you've understood both
Phase 7 and why Phase 9 exists.

---

## Step 3 — Remove the hiding place

Look at how `Pipeline` wires stages (`pipeline.py:26` and `_forward`):

```
A.worker → intermediate queue → [_forward task] → B.push() → B.input_queue → B.worker
```

**Find the moment when a frame is in neither queue.** Then: what does that mean
for any interrupt sweep you write in Step 5?

- [ ] Rewire so A's output *is* B's inbox. Delete `_forward` and `forward_tasks`.
      (Your own draft test already wires runtimes this way.)
- [ ] **Predict before running the suite:** which existing test is most likely to
      break, and why? Then run it.
- [ ] `self.output_queue` is unbounded while every other queue is bounded. Find it
      and work out what goes wrong if nobody calls `get_output()`.

**State the trade-off back:** backpressure changes shape here. What blocks now
that didn't block before?

🔍 **Checkpoint 3** — diff, plus your prediction vs. what actually happened.

---

## Step 4 — Make one runtime interrupt-safe

`interrupt()` at `runtime.py:93` has real races. The symptoms are named below —
**you find the lines and explain the mechanism.**

1. Call `stop()` and `interrupt()` concurrently and the runtime can end up
   **running again after being stopped.** Find the two lines that allow it.
   *Hint: what happens to a state check when there's an `await` after it?*
2. Two concurrent `interrupt()` calls can leave a stage with the wrong number of
   workers. Trace it.
3. A processor returning 3 frames can be cancelled after emitting only 1. Bug, or
   a semantic we accept and document? **Defend your answer.**
4. Make `processor.process` raise a `ValueError`. Observe what `runtime.state`
   reports afterwards, and what `push()` does. This is the nastiest one — state
   what's wrong in one sentence.

Then fix: a lifecycle lock, re-checking state *after* acquiring it, and enough
error handling that a dead worker stops claiming to be `RUNNING`.

Resist building retry / fallback / `ErrorFrame` machinery — that's Phase 13. The
goal here is only **"failures stop being invisible."**

You'll also notice the cancel-and-await dance appears verbatim three times.

🔍 **Checkpoint 4** — a test for each race first (including
`gather(stop(), interrupt())`), then the fix.

---

## Step 5 — `Pipeline.interrupt()`

The actual phase target. Derive the design by answering, in order:

1. Sweep source→sink (A,B,C) or sink→source (C,B,A)? **Pick one and justify it.**
   *Hint: what is A doing while you're busy cleaning B?*
2. Should `Pipeline` reach in and cancel `runtime.task` directly, or call
   `runtime.interrupt()`? This is the **ownership** question — who is allowed to
   cancel a given task? Write the rule as one sentence.
3. Barge-in detection can fire twice in quick succession. What breaks, and what do
   you need?
4. Frames already handed to the consumer via `pipeline.output_queue` — stale or
   not? What do you do with them?

Then write the post-condition as an explicit invariant — *"once `interrupt()`
returns, then ___"* — **before** implementing. Your tests assert exactly that
sentence.

Also add `Pipeline.drain()` for symmetry and make `stop()` idempotent.

🔍 **Checkpoint 5** — invariant sentence, then tests, then implementation.

---

## Step 6 — Find the hole you just left

Your sweep works for the normal case. It still has the row-3 flaw from Step 2.

**Write a test that pushes a frame from a new turn while the sweep is still in
progress, and show that your own implementation kills it.**

Mark it `@pytest.mark.xfail(strict=True)` with a docstring stating plainly why it
can't pass yet. `strict=True` matters: the day Phase 9 makes it pass, the suite
fails loudly and tells you to remove the marker.

Then answer: **why can no amount of cleverness in the sweep fix this?** What would
you have to know about a frame that you currently don't?

That answer *is* Phase 9 — derived, not handed to you.

🔍 **Checkpoint 6** — the xfail test and your answer.

---

## Step 7 — Write down what you learned

- [ ] `docs/cancellation.md` — cancel vs discard vs leave-alone; why "cancel
      everything downstream" fails; your sweep-ordering argument; the ownership
      rule; partial-output-mid-emit; the race and how generation ids dissolve it.
      WHY / WHAT / HOW / TRADE-OFFS / FAILURE MODES, in your words, with diagrams.
- [ ] `README.md` is empty. Write the project description, a runnable quickstart,
      and the roadmap with status markers.

Optional cleanup you now have the judgement for:

- `tests/test_backpressure.py` has **zero assertions**. What invariant *should* it
  assert — and is wall-clock timing a safe thing to assert on?
- `tests/test_cancellation.py` tests stdlib asyncio, not Rivet. Does it earn its
  place in the suite?

🔍 **Checkpoint 7** — docs reviewed, hand-waving challenged.

---

## Verification

```bash
uv run pytest -q             # green, 1 xfail expected
uv run pytest -q -rx         # confirm the xfail is the race, not an accident
uv run python -m rivet.main  # demo actually runs
```

Also check: no `asyncio.sleep()` used for synchronization anywhere in the suite
(`test_shutdown.py` has an `asyncio.sleep(0)` that encodes an implicit ordering
assumption; `test_cancellation.py` has a real `sleep(0.1)`).

## Commits

Two commits: hygiene, then the feature. No AI attribution — see [`AGENTS.md`](../AGENTS.md).
