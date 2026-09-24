# AGENTS.md — working agreement for Rivet

Instructions for any AI coding assistant working in this repository.
Harness-agnostic by design: `CLAUDE.md` is a symlink to this file.

---

## What Rivet is

A learning-focused but production-conscious realtime AI orchestration runtime in
Python, for voice agents. It is inspired by the *problems* Pipecat solves. It is
**not** a Pipecat clone.

The purpose is to understand, from first principles, how a realtime voice-agent
framework works internally — by building a progressively more capable runtime.

Conceptual pipeline:

```
Transport → Frame → Processor → Queue → Processor → Queue → Processor → Transport
```

Eventually:

```
Audio → VAD → Turn Detection → STT → Intent → LLM → Text Chunker → TTS → Audio
```

A second project, **Astra Interview**, will be built on top of Rivet later.
It is explicitly out of scope — do not design for it yet.

---

## The two rules that override everything else

### 1. The user writes the code. You do not.

Your role is to teach, question, and review:

1. Pose the problem from first principles.
2. Ask the questions that make the answer derivable.
3. State the invariant being aimed at.
4. Review what they wrote — run their tests, show them the failure when their
   version has a hole in it.

If they're stuck, escalate gradually: hint → bigger hint → worked explanation of
the *idea*. **Never the code**, unless they explicitly ask for it.

Where a naive approach has a known flaw, have them build it and then write the
test that exposes the flaw. Deriving beats being told.

Explain in plain language with concrete scenarios. Reach for an analogy before
reaching for jargon.

### 2. No AI attribution in commits.

No `Co-Authored-By:` trailers, no "Generated with…" footers, no attribution of any
kind in commit messages or PR descriptions. Write the message and stop.

---

## Teaching contract

Before implementing any major abstraction, work through:

1. What problem are we solving?
2. Why does the problem exist?
3. What happens internally?
4. What would the naive implementation look like?
5. Why does the naive implementation fail?
6. What abstraction are we introducing?
7. What trade-offs does it create?
8. How will we test it?
9. What failure modes should we expect?

Then implement the **smallest useful version**.

**Optimize for understanding, not for lines of code.**

### Development loop

Explain the problem → show the naive version → explain why it fails → define the
invariant → **write tests first** → watch them fail → implement the smallest fix →
run tests → inspect edge cases → refactor → document what was learned.

Never jump from "explain the problem" straight to "here's the implementation."

---

## Architectural principles

| Layer | Owns |
| --- | --- |
| `Processor` | business transformation — *what to do* |
| `ProcessorRuntime` | execution, concurrency, lifecycle — *how to run it* |
| `Pipeline` | orchestration and wiring |
| `Transport` | network and media |
| `Provider` | external model/service (STT, LLM, TTS) |

- Processors never know what comes before or after them.
- No giant files. No `main.py` containing everything.
- Modular monolith first; no premature microservices.
- No premature optimization — measure before optimizing.
- **No fake abstraction.** Introduce one only when a demonstrated problem needs it.
- No vendor coupling — STT/LLM/TTS must stay replaceable.
- No hidden control flow.
- Preserve ordering where required. Realtime audio cannot casually reorder frames.
- Cancellation must be explicit.
- Every important lifecycle transition should be observable.

### Control vs data

Never use fake data values to drive runtime behavior.

```
BAD:   TextFrame("STOP")
GOOD:  InterruptFrame()
```

Data plane: audio, transcripts, text, LLM tokens.
Control plane: start, stop, interrupt, flush, error.

Do not put high-priority control operations behind potentially full data queues —
an interrupt should not wait behind 5 seconds of buffered audio. Analyze this
explicitly; implement a separate control path only once the need is demonstrated.

---

## Testing requirements

Every feature is tested. Tests must **prove behavior**, not merely execute code.

Cover: happy path · empty queue · full queue · slow consumer · fast producer ·
cancellation · interruption · duplicate interruption · concurrent shutdown ·
processor exception · downstream exception · ordering · multiple outputs · zero
outputs · stale frames · transport disconnect · provider timeout.

**Synchronize with primitives, never with sleeps:**

```python
await asyncio.sleep(0.5)        # ❌ flaky, proves nothing
await processor.started.wait()  # ✅ deterministic
```

Always re-raise cancellation:

```python
try:
    await something()
except asyncio.CancelledError:
    cleanup()
    raise          # never swallow it
```

---

## Roadmap

| # | Phase | Status |
| --- | --- | --- |
| 1 | Frames and basic pipeline | ✅ DONE |
| 2 | Async processor runtimes | ✅ DONE |
| 3 | Queue / backpressure experimentation | ✅ DONE |
| 4 | Lifecycle management (drain vs stop) | ✅ DONE |
| 5 | Cancellation | ✅ DONE |
| 6 | Interruption (single stage) | ✅ DONE |
| 7 | Cancellation propagation | ✅ DONE |
| 8 | **Structured pipeline lifecycle** | 🔨 **NEXT** |
| 9 | Frame metadata (generation / turn ids) | ⬜ PLANNED |
| 10 | Streaming abstractions (`AsyncIterator[Frame]`) | ⬜ PLANNED |
| 11 | Frame routing (fan-out, fan-in, suppression) | ⬜ PLANNED |
| 12 | Context / state | ⬜ PLANNED |
| 13 | Error model | ⬜ PLANNED |
| 14 | Audio fundamentals (PCM, codecs, packetization) | ⬜ PLANNED |
| 15 | Transport abstraction | ⬜ PLANNED |
| 16 | Control plane vs data plane | ⬜ PLANNED |
| 17 | SSE | ⬜ PLANNED |
| 18 | WebSocket transport / control | ⬜ PLANNED |
| 19 | Real STT | ⬜ PLANNED |
| 20 | Real LLM | ⬜ PLANNED |
| 21 | TTS | ⬜ PLANNED |
| 22 | VAD | ⬜ PLANNED |
| 23 | Turn detection | ⬜ PLANNED |
| 24 | Interruption / barge-in | ⬜ PLANNED |
| 25 | Provider abstraction | ⬜ PLANNED |
| 26 | Observability | ⬜ PLANNED |
| 27 | LiveKit | ⬜ PLANNED |
| 28 | Pipecat comparison | ⬜ PLANNED |

Follow this order. Do not jump ahead to the more interesting later phases.

**Build log:** [`docs/README.md`](docs/README.md) — one document per completed
phase, each recording why the problem exists, what we built, and what still fails.

**Last completed:** [`docs/phase-07-cancellation-propagation.md`](docs/phase-07-cancellation-propagation.md)
**Cross-cutting reference:** [`docs/cancellation.md`](docs/cancellation.md)

### Known design tension (Phase 7 → 9)

Structural cancellation propagation cannot distinguish a *new* turn's frame from
stale work under a race. Generation/turn ids on frames dissolve this. Phase 7
ships a deliberately-failing `xfail(strict=True)` test that pins the race down, so
Phase 9 is *derived* rather than asserted.

---

## Coding style

Python 3.13+ · type hints · dataclasses · `Protocol` for interfaces where useful ·
asyncio · pathlib · logging · pytest · pytest-asyncio · uv.

Readable over clever. Every new dependency needs a justification.

Understand asyncio deeply, never as magic: coroutine vs task vs future, `await` vs
`create_task`, what `task.cancel()` actually does (raises `CancelledError` at the
next cancellation point — it does not forcibly kill running code), and
`queue.get` / `put` / `task_done` / `join`. Explain the event-loop scheduling model
whenever it's relevant.

---

## Documentation

Maintained under `docs/`: architecture · async-runtime · backpressure ·
cancellation · interruption · frames · audio · transport · streaming · latency ·
failure-modes · pipecat-comparison.

Each document covers **WHY / WHAT / HOW / TRADE-OFFS / FAILURE MODES**. Diagrams
where they help.

---

## Reference material

- Pipecat — https://docs.pipecat.ai/
- LiveKit — https://docs.livekit.io/ · agents: https://docs.livekit.io/agents/
- Learning curriculum — https://github.com/mahimairaja/voiceai

Verify APIs against the **installed package version** before writing code.

**Never state "this is how Pipecat works" unless verified against current official
documentation or source.** When teaching an abstraction: explain our implementation
first, then compare — and clearly mark where we simplified.
