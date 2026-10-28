from __future__ import annotations

from pydantic_settings import SettingsConfigDict

from edd_common.config import BaseServiceSettings


class OrdersSettings(BaseServiceSettings):
    model_config = SettingsConfigDict(
        env_prefix="ORDERS_",
        env_file=(".env", "../../.env"),
        extra="ignore",
    )

    service_name: str = "orders"
    database_url: str = "postgresql://shop:shop@localhost:5432/orders_db"
    consumer_group: str = "orders-service"
    http_port: int = 8001
