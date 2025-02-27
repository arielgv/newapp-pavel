import urllib.parse
from typing import AsyncGenerator, Optional
from google.cloud.sql.connector import Connector, IPTypes
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
from app.core.config import settings
from app.utils.logger import logger


class DatabaseManager:
    __engine: Optional[Engine] = None
    __session_local: Optional[sessionmaker] = None

    @classmethod
    def get_engine(cls) -> Engine:
        """Lazy initialization of the database engine."""
        if cls.__engine is None:
            encoded_password = urllib.parse.quote_plus(settings.db_password)
            if settings.ENVIRONMENT == "local":
                # Local database engine
                cls.__engine = create_engine(
                    f"postgresql+pg8000://{settings.db_user}:{encoded_password}@{settings.db_host}:{settings.db_port}/{settings.db_name}",
                    pool_size=settings.db_pool_size,
                    max_overflow=settings.db_max_overflow,
                    pool_timeout=settings.db_pool_timeout,
                    pool_recycle=settings.db_pool_recycle,
                    future=True,
                    echo=settings.db_echo,
                )
            else:
                # Cloud SQL database engine with Google Connector
                connector = Connector()

                def get_conn():
                    return connector.connect(
                        settings.instance_connection_name,
                        "pg8000",
                        user=settings.db_user,
                        password=urllib.parse.unquote(encoded_password),
                        db=settings.db_name,
                        ip_type=IPTypes.PUBLIC,
                    )

                cls.__engine = create_engine(
                    "postgresql+pg8000://",
                    creator=get_conn,
                    pool_size=settings.db_pool_size,
                    max_overflow=settings.db_max_overflow,
                    pool_timeout=settings.db_pool_timeout,
                    pool_recycle=settings.db_pool_recycle,
                    future=True,
                    echo=settings.db_echo,
                )

        return cls.__engine

    @classmethod
    def get_session_local(cls) -> sessionmaker:
        """Lazy initialization of session maker."""
        if cls.__session_local is None:
            cls.__session_local = sessionmaker(
                autocommit=False, autoflush=False, bind=cls.get_engine()
            )
        return cls.__session_local

    @classmethod
    async def get_db(cls) -> AsyncGenerator:
        """
        Get a database session for FastAPI dependency injection.
        Uses async-compatible generator.
        """
        session = cls.get_session_local()()
        try:
            yield session
        except Exception as e:
            logger.error(f"Database session error: {e}")
            session.rollback()
            raise e
        finally:
            session.close()
