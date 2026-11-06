class SimulatedCrash(Exception):
    """Raised by tests and the demo to stop a process at a chosen point."""


class PermanentError(Exception):
    """Retrying cannot fix this message. It goes to the dead-letter topic at once."""
