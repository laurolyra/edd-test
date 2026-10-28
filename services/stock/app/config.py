from __future__ import annotations

from pydantic_settings import SettingsConfigDict

from edd_common.config import BaseServiceSettings


class StockSettings(BaseServiceSettings):
    model_config = SettingsConfigDict(
        env_prefix="STOCK_",
        env_file=(".env", "../../.env"),
        extra="ignore",
    )

    service_name: str = "stock"
    database_url: str = "postgresql://shop:shop@localhost:5432/stock_db"
    consumer_group: str = "stock-service"
    http_port: int = 8002
