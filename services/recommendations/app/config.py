from __future__ import annotations

from pydantic_settings import SettingsConfigDict

from edd_common.config import BaseServiceSettings


class RecommendationsSettings(BaseServiceSettings):
    model_config = SettingsConfigDict(
        env_prefix="RECOMMENDATIONS_",
        env_file=(".env", "../../.env"),
        extra="ignore",
    )

    service_name: str = "recommendations"
    database_url: str = "postgresql://shop:shop@localhost:5432/recommendations_db"
    consumer_group: str = "recommendations-service"
    http_port: int = 8005
