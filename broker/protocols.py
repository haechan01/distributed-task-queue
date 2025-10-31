from typing import Protocol, Callable, Dict, Any


class Transport(Protocol):
    """Message transport abstraction used by nodes.
    """

    def send(self, to: str, msg: Dict[str, Any]) -> None:
        """Send `msg` to peer `to`.

        `msg` is a JSON-serializable dict. Implementations may add metadata
        (e.g., headers) but should preserve the payload.
        """
        raise NotImplementedError

    def register(self, node_id: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        """Register a receive handler for `node_id`.

        The handler is called with the decoded message dict upon delivery.
        """
        raise NotImplementedError


class SchedulerCancel(Protocol):
    """Callable returned by `Scheduler.call_later` to cancel a pending event."""

    def __call__(self) -> None:
        raise NotImplementedError


class Scheduler(Protocol):
    """Scheduler abstraction; used by nodes to schedule future work."""

    def call_later(self, ms: int, cb: Callable[[], None]) -> SchedulerCancel:
        """Schedule callback `cb` to run in `ms` milliseconds."""
        raise NotImplementedError

    def now_ms(self) -> int:
        """Return current time in milliseconds for this scheduler domain."""
        raise NotImplementedError
