"""Harbor ingestion module.

Minimal first version: validates incoming telemetry events and
normalises them into the internal record format.
"""

from __future__ import annotations

import time

# Metric names we accept. Anything else is rejected so that a single
# misbehaving service cannot flood the pipeline with junk keys.
ALLOWED_METRICS = frozenset({"requests", "latency_ms", "errors", "events_ingested"})

# Hard cap on event size, applied before any parsing happens.
MAX_PAYLOAD_BYTES = 32 * 1024


class ValidationError(ValueError):
    """Raised when a telemetry event does not pass validation."""


def _normalise_name(name: str) -> str:
    """Lowercase and trim a metric name."""
    return name.strip().lower()


def validate_event(event: dict) -> dict:
    """Validate one telemetry event and return a normalised copy.

    An event must contain:
      * ``name``   - a non-empty string metric name from ALLOWED_METRICS
      * ``value``  - a finite int/float (never bool, never None)
      * optional ``source`` string

    Returns a new dict with the normalised metric name, the value, the
    source (``unknown`` when absent), and a unix-style ``received_at``
    timestamp.
    """
    if not isinstance(event, dict):
        raise ValidationError("event must be a dict")

    name = event.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ValidationError("event must have a non-empty string 'name'")

    name = _normalise_name(name)
    if name not in ALLOWED_METRICS:
        raise ValidationError(f"unknown metric name: {name!r}")

    value = event.get("value")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError("event value must be a number")

    source = event.get("source")
    if source is not None and not isinstance(source, str):
        raise ValidationError("event source must be a string when provided")

    return {
        "name": name,
        "value": value,
        "source": source if source else "unknown",
        "received_at": int(time.time()),
    }


def ingest(events: list[dict]) -> tuple[list[dict], list[tuple[int, dict, str]]]:
    """Validate a batch of events.

    Returns a ``(accepted, rejected)`` pair where ``accepted`` is the list
    of normalised records and ``rejected`` holds ``(index, event, reason)``
    triples so the caller can log or drop the bad ones.
    """
    accepted: list[dict] = []
    rejected: list[tuple[int, dict, str]] = []

    for index, event in enumerate(events):
        if not isinstance(event, dict):
            rejected.append((index, event, "event is not a dict"))
            continue
        try:
            accepted.append(validate_event(event))
        except ValidationError as exc:
            rejected.append((index, event, str(exc)))

    return accepted, rejected
