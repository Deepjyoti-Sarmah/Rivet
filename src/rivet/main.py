import asyncio

from rivet.frames import TextFrame
from rivet.pipeline import Pipeline
from rivet.processors.debug import DebugProcessor


async def main():
    pipeline = Pipeline(
        [
            DebugProcessor(),
        ]
    )

    result = await pipeline.push(TextFrame("hello rivet"))

    print("Output:", result)


if __name__ == "__main__":
    asyncio.run(main())
