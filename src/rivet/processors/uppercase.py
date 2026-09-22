from rivet.frames import Frame, TextFrame
from rivet.processor import Processor


class UppercaseProcessor(Processor):
    async def process(self, frame: Frame) -> list[Frame]:
        if isinstance(frame, TextFrame):
            return [TextFrame(frame.text.upper())]

        return [frame]
