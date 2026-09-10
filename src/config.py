"""Application configuration using Pydantic Settings.

Author: @pushkarreddyy
Repository: https://github.com/pushkarreddyy/api-rate-limiter-proxy
"""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings loaded from environment variables / .env file.

    All settings can be overridden via environment variables or a .env file
    in the project root.
    """

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    # Upstream target
    UPSTREAM_BASE_URL: str = "http://localhost:9000"

    # Rate limiting
    RATE_LIMIT_MAX_REQUESTS: int = 100
    RATE_LIMIT_WINDOW_SECONDS: int = 60

    # Proxy
    PROXY_TIMEOUT_SECONDS: float = 30.0

    # Logging
    LOG_LEVEL: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


@lru_cache
def get_settings() -> Settings:
    """Return cached settings instance."""
    return Settings()
