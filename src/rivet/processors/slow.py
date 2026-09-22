import asyncio
import time

from rivet.frames import Frame
from rivet.processor import Processor


class SlowProcessor(Processor):
    async def process(self, frame: Frame) -> list[Frame]:
        start = time.perf_counter()

        print(f"START {frame}")

        await asyncio.sleep(1)

        elapsed = time.perf_counter() - start

        print(f"END {frame} ({elapsed:.3f}s)")

        return [frame]
