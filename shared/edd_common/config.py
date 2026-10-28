from __future__ import annotations

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseServiceSettings(BaseSettings):
    """Common settings shared by every service.

    Each service subclasses this and sets an ``env_prefix`` (for example
    ``ORDERS_``) plus its own database URL and consumer group. Kafka settings
    are deliberately global because every process talks to the same broker.
    """

    model_config = SettingsConfigDict(
        extra="ignore",
        env_file=(".env", "../../.env"),
        env_file_encoding="utf-8",
    )

    service_name: str = "service"
    database_url: str = ""
    consumer_group: str = ""
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        validation_alias=AliasChoices("KAFKA_BOOTSTRAP_SERVERS"),
    )
    outbox_poll_interval_seconds: float = 1.0
    outbox_batch_size: int = 100
    consumer_max_retries: int = 3
    log_level: str = "INFO"
