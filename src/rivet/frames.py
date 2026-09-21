from dataclasses import dataclass
from enum import Enum


class FrameType(str, Enum):
    AUDIO = "audio"
    TEXT = "text"
    TRANSCRIPT = "transcript"
    LLM_TOKEN = "llm_token"
    ERROR = "error"
    INTERRUPT = "interrupt"


@dataclass(slots=True)
class Frame:
    type: FrameType


@dataclass(slots=True)
class AudioFrame(Frame):
    data: bytes
    sample_rate: int
    channels: int

    def __init__(
        self,
        data: bytes,
        sample_rate: int,
        channels: int,
    ) -> None:
        super().__init__(FrameType.AUDIO)
        self.data = data
        self.sample_rate = sample_rate
        self.channels = channels


@dataclass(slots=True)
class TextFrame(Frame):
    text: str

    def __init__(self, text: str) -> None:
        super().__init__(FrameType.TEXT)
        self.text = text


@dataclass(slots=True)
class TranscriptFrame(Frame):
    text: str
    final: bool

    def __init__(self, text: str, final: bool) -> None:
        super().__init__(type=FrameType.TRANSCRIPT)
        self.text = text
        self.final = final


@dataclass(slots=True)
class LLMTokenFrame(Frame):
    token: str

    def __init__(self, token: str) -> None:
        super().__init__(type=FrameType.LLM_TOKEN)
        self.token = token


@dataclass(slots=True)
class InturuptFrame(Frame):
    reason: str

    def __init__(self, reason: str) -> None:
        super().__init__(type=FrameType.INTERRUPT)
        self.reason = reason


@dataclass(slots=True)
class ErrorFrame(Frame):
    error: Exception

    def __init__(self, error: Exception) -> None:
        self.error = error
