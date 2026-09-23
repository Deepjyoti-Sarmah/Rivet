import asyncio

from rivet.frames import TextFrame
from rivet.pipeline import Pipeline
from rivet.processors.debug import DebugProcessor
from rivet.processors.exclamation import ExclamationProcessor
from rivet.processors.uppercase import UppercaseProcessor


async def main() -> None:
    pipeline = Pipeline(
        [
            DebugProcessor(),
            UppercaseProcessor(),
            ExclamationProcessor(),
        ]
    )

    await pipeline.start()

    await pipeline.push(TextFrame("hello rivet"))
    output = await pipeline.get_output()

    print("Output:", output)

    await pipeline.stop()


if __name__ == "__main__":
    asyncio.run(main())
