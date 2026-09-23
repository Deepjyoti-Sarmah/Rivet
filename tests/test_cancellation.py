import asyncio

import pytest


async def worker():
    print("worker: started")

    try:
        await asyncio.sleep(10)
        print("worker: finished")

    except asyncio.CancelledError:
        print("worker: cancelled")
        raise


@pytest.mark.asyncio
async def test_task_cancellation():
    task = asyncio.create_task(worker())

    await asyncio.sleep(0.1)

    print("test: cancelling worker")

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
