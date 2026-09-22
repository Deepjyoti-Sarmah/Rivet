from __future__ import annotations

from abc import ABC, abstractmethod

from rivet.frames import Frame


class Processor(ABC):
    @abstractmethod
    async def process(self, frame: Frame) -> list[Frame]:
        """
        Consume one frame and return zero or more output frames.
        """
        raise NotImplementedError
