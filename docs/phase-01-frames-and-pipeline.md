# Phase 1 — Frames and the basic pipeline

**Status:** ✅ done
**Code:** `src/rivet/frames.py`, `src/rivet/processor.py`

---

## WHY

A voice agent moves many *kinds* of thing through the same machinery: microphone
audio, partial transcripts, final transcripts, LLM tokens, synthesised audio,
errors, interrupts.

The naive design gives each stage its own bespoke signature:

```python
async def transcribe(audio: bytes) -> str: ...
async def generate(text: str) -> str: ...
async def synthesize(text: str) -> bytes: ...
```

This fails the moment you want generic infrastructure. You cannot write *one*
queue, *one* worker loop, or *one* interrupt mechanism that works for all three,
because every stage speaks a different type. Every new capability — buffering,
cancellation, metrics, logging — has to be written N times.

## WHAT

**A `Frame` is the single envelope type that moves through the system.**

```
Frame = one piece of data or one event travelling through the pipeline
```

The runtime deliberately does **not** understand the business meaning of most
frames. It moves envelopes; processors read the contents.

```python
@dataclass(slots=True)
class Frame:
    type: FrameType = field(init=False)
```

Subclasses carry the payload — `TextFrame(text)`, `AudioFrame(data, sample_rate,
channels)`, `TranscriptFrame(text, final)`, `LLMTokenFrame(token)`,
`InterruptFrame(reason)`, `ErrorFrame(error)`.

And **a `Processor` is a pure transformation over frames**:

```python
class Processor(ABC):
    @abstractmethod
    async def process(self, frame: Frame) -> list[Frame]:
        ...
```

That signature answers exactly one question: *"what should happen to this frame?"*

```
TextFrame("hello")  →  UppercaseProcessor  →  TextFrame("HELLO")
```

## HOW

`list[Frame]` as the return type is doing real work. It permits:

| Return | Meaning |
| --- | --- |
| `[frame]` | pass through / transform |
| `[]` | **suppress** — swallow the frame |
| `[a, b, c]` | **expand** — one input, many outputs |

That last one matters more than it looks. An LLM stage turns one prompt into many
token frames; a chunker turns many tokens into a few sentence frames. Fan-out and
fan-in are already expressible.

**The critical constraint: a processor never knows what comes before or after it.**
It has no reference to its neighbours, no queue, no task. That decoupling is what
makes Phases 2–7 possible at all — the runtime can be rebuilt underneath a
processor without the processor changing.

## TRADE-OFFS

**`type` tag alongside subclassing is redundant.** We have both `isinstance()` and
`frame.type`. Kept because a tag is cheap to switch on and will serialise across a
transport boundary later (Phase 15), where Python classes won't survive.

**`slots=True`** keeps frames small. At 50 audio frames/second across a long
session, per-object dict overhead is not nothing.

**No `DataFrame` / `ControlFrame` split yet.** The conceptual hierarchy is known:

```
Frame
├── DataFrame      audio, text, transcript, tokens
└── ControlFrame   start, stop, interrupt, flush, error
```

Deliberately *not* built. No code branches on the distinction yet, so the
hierarchy would be decoration. It gets introduced when something demands it —
most likely Phase 16, where control genuinely needs to bypass a full data queue.

**`list[Frame]` is not streaming.** A processor must finish completely before
emitting anything. For an LLM this is exactly wrong: you want token 1 forwarded
while token 50 is still being generated. This is a known, accepted limitation —
Phase 10 replaces it with `AsyncIterator[Frame]` once we have felt the pain.

## FAILURE MODES

- **`Frame.type` is `init=False` with no default.** The base class is
  instantiable, but with `slots=True` and no `__post_init__`, reading `.type`
  raises `AttributeError`. Only subclasses are safe to construct.
- **No frame identity.** Frames carry no timestamp, sequence number, session, or
  turn id. Two `TextFrame("hello")` instances are indistinguishable. Harmless now;
  it becomes the central problem in Phase 7 and is solved in Phase 9.
- **`ErrorFrame` exists but nothing constructs it.** Phase 13.

## WHAT WE LEARNED

A uniform envelope type is what lets orchestration be written *once*. The runtime's
ignorance of frame semantics is a feature, not laziness — it's precisely why one
queue implementation serves audio, text, and tokens alike.
