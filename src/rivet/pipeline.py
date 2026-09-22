from __future__ import annotations

from rivet.frames import Frame
from rivet.processor import Processor


class Pipeline:
    def __init__(self, processors: list[Processor]) -> None:
        self.processors = processors

    async def push(self, frame: Frame) -> list[Frame]:
        frames = [frame]

        for processor in self.processors:
            next_frames: list[Frame] = []

            for current in frames:
                output = await processor.process(current)
                next_frames.extend(output)

            frames = next_frames

        return frames
