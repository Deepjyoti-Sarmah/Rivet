import pytest

from rivet.frames import TextFrame
from rivet.pipeline import Pipeline
from rivet.processors.exclamation import ExclamationProcessor
from rivet.processors.uppercase import UppercaseProcessor


@pytest.mark.asyncio
async def test_pipeline_runtime():
    pipeline = Pipeline(
        [
            UppercaseProcessor(),
            ExclamationProcessor(),
        ]
    )

    await pipeline.start()

    await pipeline.push(TextFrame("hello"))

    result = await pipeline.get_output()

    assert isinstance(result, TextFrame)
    assert result.text == "HELLO!"

    await pipeline.stop()
