import asyncio

from rivet.frames import Frame, TextFrame
from rivet.processor import Processor


class SlowProcessor(Processor):
    async def process(self, frame: Frame) -> list[Frame]:
        print("SlowProcessor: stated")

        await asyncio.sleep(2)

        print("SlowProcessor: finished")

        if isinstance(frame, TextFrame):
            return [TextFrame(frame.text.upper())]

        return [frame]
