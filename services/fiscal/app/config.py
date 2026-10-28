from __future__ import annotations

from pydantic_settings import SettingsConfigDict

from edd_common.config import BaseServiceSettings


class FiscalSettings(BaseServiceSettings):
    model_config = SettingsConfigDict(
        env_prefix="FISCAL_",
        env_file=(".env", "../../.env"),
        extra="ignore",
    )

    service_name: str = "fiscal"
    database_url: str = "postgresql://shop:shop@localhost:5432/fiscal_db"
    consumer_group: str = "fiscal-service"
    http_port: int = 8004
