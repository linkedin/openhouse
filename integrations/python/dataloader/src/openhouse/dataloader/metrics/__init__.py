"""OpenTelemetry metrics infrastructure for the dataloader.

This package depends only on ``opentelemetry-api``, which provides a no-op
fallback when no SDK is configured.  The *application* (not this library)
is responsible for installing an SDK and configuring exporters.

Call sites should obtain a ``Meter`` via the OTEL API directly::

    from opentelemetry.metrics import get_meter
    from openhouse.dataloader.metrics import METER_NAME

    meter = get_meter(METER_NAME)
"""

METER_NAME = "OpenHouse.DataLoader"

__all__ = ["METER_NAME"]
