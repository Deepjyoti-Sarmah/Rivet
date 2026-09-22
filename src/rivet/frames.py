from __future__ import annotations

from dataclasses import dataclass, field
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
    type: FrameType = field(init=False)


@dataclass(slots=True)
class TextFrame(Frame):
    text: str

    def __post_init__(self) -> None:
        self.type = FrameType.TEXT


@dataclass(slots=True)
class TranscriptFrame(Frame):
    text: str
    final: bool

    def __post_init__(self) -> None:
        self.type = FrameType.TRANSCRIPT


@dataclass(slots=True)
class LLMTokenFrame(Frame):
    token: str

    def __post_init__(self) -> None:
        self.type = FrameType.LLM_TOKEN


@dataclass(slots=True)
class InturuptFrame(Frame):
    reason: str

    def __post_init__(self) -> None:
        self.type = FrameType.INTERRUPT


@dataclass(slots=True)
class ErrorFrame(Frame):
    error: Exception

    def __post_init__(self) -> None:
        self.type = FrameType.ERROR


@dataclass(slots=True)
class AudioFrame(Frame):
    data: bytes
    sample_rate: int
    channels: int

    # breakpoint()

    def __post_init__(
        self,
    ) -> None:
        self.type = FrameType.AUDIO
