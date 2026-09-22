from rivet.frames import Frame, TextFrame
from rivet.processor import Processor


class ExclamationProcessor(Processor):
    async def process(self, frame: Frame) -> list[Frame]:
        if isinstance(frame, TextFrame):
            return [TextFrame(frame.text + "!")]

        return [frame]
