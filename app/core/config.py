from typing import Optional
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import os

load_dotenv()

class Settings(BaseSettings):
    app_name: str = "COSA Core Engine"
    debug: bool = True
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "local")

    # Database Configuration
    db_user: str = os.getenv("DB_USER")
    db_password: str = os.getenv("DB_PASSWORD")
    db_host: str = os.getenv("DB_HOST")
    db_port: int = int(os.getenv("DB_PORT", 5432))
    db_name: str = os.getenv("DB_NAME")
    db_table_prefix: str = os.getenv("DB_TABLE_PREFIX", "core")
    instance_connection_name: str = os.getenv("INSTANCE_CONNECTION_NAME", "")

    # Database Pooling Configuration
    db_pool_size: int = 10
    db_max_overflow: int = 5
    db_pool_timeout: int = 60
    db_pool_recycle: int = 3600
    db_echo: bool = False


settings = Settings()