"""Shared building blocks for the event-driven shop.

The package intentionally keeps the infrastructure small and explicit so it is
easy to study: an event envelope, a transactional outbox, an idempotent
consumer, and a tiny service runtime that wires them together.
"""

from edd_common.config import BaseServiceSettings
from edd_common.events import Event, new_event

__all__ = ["BaseServiceSettings", "Event", "new_event"]
