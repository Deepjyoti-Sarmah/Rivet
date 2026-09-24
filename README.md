# Rivet

A realtime AI orchestration runtime for voice agents, built from first principles.

Rivet is a learning project. It is inspired by the *problems* frameworks like
Pipecat solve — it is not a clone of one. The goal is to understand, by building,
how a realtime voice-agent framework works internally.

Every abstraction here exists because a demonstrated problem required it, and
[`docs/`](docs/README.md) records the problem before the solution.

---

## The shape

```
Transport → Frame → Processor → Queue → Processor → Queue → Processor → Transport
```

Eventually:

```
Audio → VAD → Turn Detection → STT → Intent → LLM → Text Chunker → TTS → Audio
```

Three layers, kept apart:

```
Processor          what to do            business transformation
ProcessorRuntime   how to run it         queues, worker task, lifecycle
Pipeline           wiring and ordering   orchestration
```

A processor never knows what comes before or after it.

---

## Quickstart

```bash
uv sync
uv run pytest -q
uv run python -m rivet.main
```

```
[DEBUG] TextFrame(type=<FrameType.TEXT: 'text'>, text='hello rivet')
Output: TextFrame(type=<FrameType.TEXT: 'text'>, text='HELLO RIVET!')
```

### Building a pipeline

```python
from rivet.frames import TextFrame
from rivet.pipeline import Pipeline
from rivet.processors.uppercase import UppercaseProcessor
from rivet.processors.exclamation import ExclamationProcessor

pipeline = Pipeline([UppercaseProcessor(), ExclamationProcessor()])
await pipeline.start()

await pipeline.push(TextFrame("hello"))
print(await pipeline.get_output())     # TextFrame(text='HELLO!')

await pipeline.stop()
```

### Writing a processor

```python
class UppercaseProcessor(Processor):
    async def process(self, frame: Frame) -> list[Frame]:
        if isinstance(frame, TextFrame):
            return [TextFrame(frame.text.upper())]
        return [frame]
```

`list[Frame]` lets one input become none (`[]`, suppress), one (`[frame]`), or
many (`[a, b, c]`, expand).

### Lifecycle

```python
await pipeline.stop()       # cancel now, pipeline ends
await pipeline.interrupt()  # discard current + queued work, pipeline stays alive
```

`interrupt()` is the barge-in verb: keep the machine, drop the work.

---

## Status

Phases 1–7 are implemented: frames, async processor runtimes, bounded queues and
backpressure, lifecycle (`drain` vs `stop`), cancellation, interruption, and
cancellation propagation across a chain.

| # | Phase | |
| --- | --- | --- |
| 1 | Frames and basic pipeline | ✅ |
| 2 | Async processor runtimes | ✅ |
| 3 | Queue / backpressure | ✅ |
| 4 | Lifecycle management | ✅ |
| 5 | Cancellation | ✅ |
| 6 | Interruption | ✅ |
| 7 | Cancellation propagation | ✅ |
| 8 | Structured pipeline lifecycle | ⬜ |
| 9 | Frame metadata (generation ids) | ⬜ |
| 10 | Streaming abstractions | ⬜ |
| 11 | Frame routing | ⬜ |
| 12 | Context / state | ⬜ |
| 13 | Error model | ⬜ |
| 14 | Audio fundamentals | ⬜ |
| 15 | Transport abstraction | ⬜ |
| 16 | Control plane vs data plane | ⬜ |
| 17–18 | SSE, WebSocket | ⬜ |
| 19–21 | STT, LLM, TTS | ⬜ |
| 22–24 | VAD, turn detection, barge-in | ⬜ |
| 25–26 | Provider abstraction, observability | ⬜ |
| 27–28 | LiveKit, Pipecat comparison | ⬜ |

Full roadmap in [`AGENTS.md`](AGENTS.md).

### Known limitation

The interrupt sweep decides what is stale by **timing**, so a frame arriving
during the sweep can be deleted along with the work it was meant to replace. The
race is pinned by an `xfail(strict=True)` test and closed in Phase 9 by stamping
frames with a generation id. See [`docs/cancellation.md`](docs/cancellation.md).

---

## Docs

[`docs/`](docs/README.md) — one document per phase, each covering why the problem
exists, what was built, the trade-offs taken, and the failure modes still open.
Each phase's failure modes are the next phase's reason to exist.

- [Cancellation, interruption, and stale work](docs/cancellation.md) — cross-cutting reference

---

## Requirements

Python 3.12+, [uv](https://docs.astral.sh/uv/). No runtime dependencies.
