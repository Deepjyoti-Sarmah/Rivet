import time

import pytest

from rivet.frames import TextFrame
from rivet.pipeline import Pipeline
from rivet.processors.slow import SlowProcessor

# @pytest.mark.asyncio
# async def test_queue_backpressure():
#     pipeline = Pipeline(
#         [
#             SlowProcessor(),
#         ],
#         max_queue_size=2,
#     )

#     await pipeline.start()

#     start = time.perf_counter()

#     await pipeline.push(TextFrame("one"))
#     await pipeline.push(TextFrame("two"))
#     await pipeline.push(TextFrame("three"))

#     elapsed = time.perf_counter() - start

#     print(f"push elapsed: {elapsed:.2f}s")

#     await pipeline.stop()

#     print(f"total push time: {elapsed:.2f}s")


@pytest.mark.asyncio
async def test_queue_backpressure():
    pipeline = Pipeline(
        [
            SlowProcessor(),
        ],
        max_queue_size=2,
    )

    await pipeline.start()

    for i in range(10):
        start = time.perf_counter()

        await pipeline.push(TextFrame(str(i)))

        elapsed = time.perf_counter() - start

        print(f"push {i}: {elapsed:.3f}s")

    await pipeline.stop()
