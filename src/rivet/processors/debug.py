from src.rivet.frames import Frame
from src.rivet.processor import Processor


class DebugProcessor(Processor):
    async def process(self, frame: Frame) -> list[Frame]:
        print(f"[DEBUG] {frame}")
        return [frame]
