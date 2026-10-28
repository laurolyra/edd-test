from __future__ import annotations

from pydantic_settings import SettingsConfigDict

from edd_common.config import BaseServiceSettings


class InvoicesSettings(BaseServiceSettings):
    model_config = SettingsConfigDict(
        env_prefix="INVOICES_",
        env_file=(".env", "../../.env"),
        extra="ignore",
    )

    service_name: str = "invoices"
    database_url: str = "postgresql://shop:shop@localhost:5432/invoices_db"
    consumer_group: str = "invoices-service"
    http_port: int = 8003
