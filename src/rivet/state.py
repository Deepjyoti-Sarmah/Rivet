from enum import Enum


class RuntimeState(Enum):
    CREATED = "created"
    RUNNING = "running"
    DRAINING = "draining"
    STOPPING = "stopping"
    STOPPED = "stopped"
    FAILED = "failed"


class Generation:
    """Turn counter shared by every stage in a pipeline."""

    def __init__(self) -> None:
        self.value = 0

    def bump(self) -> int:
        self.value += 1
        return self.value

    def is_stale(self, generation: int) -> bool:
        return generation < self.value
