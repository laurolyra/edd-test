from __future__ import annotations

import logging
import sys

from edd_common.config import BaseServiceSettings


class ServiceLogFormatter(logging.Formatter):
    """Small, readable formatter that always includes the service name."""

    def __init__(self, service_name: str) -> None:
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        timestamp = self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z")
        return (
            f"{timestamp} {record.levelname:<8} [{self.service_name}] "
            f"{record.name}: {record.getMessage()}"
        )


def configure_logging(settings: BaseServiceSettings) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ServiceLogFormatter(settings.service_name))
    logging.basicConfig(
        level=settings.log_level.upper(),
        handlers=[handler],
        force=True,
    )
    # aiokafka is chatty at INFO; warnings are enough for this project.
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
