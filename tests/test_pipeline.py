import pytest

from rivet.frames import Frame, TextFrame
from rivet.pipeline import Pipeline
from rivet.processor import Processor


class UppercaseProcessor(Processor):
    async def process(self, frame: Frame) -> list[Frame]:
        if isinstance(frame, TextFrame):
            return [TextFrame(frame.text.upper())]

        return [frame]


class ExclamationProcessor(Processor):
    async def process(self, frame: Frame) -> list[Frame]:
        if isinstance(frame, TextFrame):
            return [TextFrame(frame.text + "!")]

        return [frame]


@pytest.mark.asyncio
async def test_pipeline():
    pipeline = Pipeline(
        [
            UppercaseProcessor(),
            ExclamationProcessor(),
        ]
    )

    result = await pipeline.push(TextFrame("hello"))

    assert len(result) == 1
    assert result[0].text == "HELLO!"
