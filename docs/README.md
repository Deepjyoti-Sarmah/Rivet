# Rivet docs

Build log for a realtime voice-agent orchestration runtime, written from first
principles. Each phase document records **why** a problem exists, **what** we
built, **how** it works, the **trade-offs** taken, and the **failure modes** that
remain.

Read them in order — each phase exists because the previous one broke.

| Phase | Document | Status |
| --- | --- | --- |
| 1 | [Frames and the basic pipeline](phase-01-frames-and-pipeline.md) | ✅ |
| 2 | [The processor runtime](phase-02-processor-runtime.md) | ✅ |
| 3 | [Queues and backpressure](phase-03-backpressure.md) | ✅ |
| 4 | [Lifecycle: drain vs stop](phase-04-lifecycle.md) | ✅ |
| 5 | [Cancellation](phase-05-cancellation.md) | ✅ |
| 6 | [Interruption](phase-06-interruption.md) | ✅ |
| 7 | [Cancellation propagation](phase-07-cancellation-propagation.md) | ✅ |

Cross-cutting: [Cancellation, interruption, and stale work](cancellation.md).

The full 28-phase roadmap lives in [`AGENTS.md`](../AGENTS.md).

## The through-line

Every phase is one answer to the same question, asked at increasing scale:

> **When work becomes worthless mid-flight, how do you get rid of it?**

In a voice agent, work becomes worthless constantly — the user interrupts, changes
their mind, or talks over the answer. A batch system can ignore this. A realtime
one cannot: the cost of finishing dead work is a bot that keeps talking over you.

```
Phase 1-2   How does work flow at all?
Phase 3     What if the producer outruns the consumer?
Phase 4     How does work stop cleanly?
Phase 5     How does work stop *immediately*?
Phase 6     How does one stage discard work and stay alive?
Phase 7     How does a whole chain discard work?
Phase 9     How do you tell dead work from new work? ← next
```
