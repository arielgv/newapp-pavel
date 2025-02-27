from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy import MetaData
from app.core.config import settings
import re


# Create a shared metadata object
metadata = MetaData(schema="public")


@as_declarative(metadata=metadata)
class Base:
    id: int
    __name__: str

    @declared_attr
    def __tablename__(cls) -> str:
        # Convert CamelCase to snake_case
        snake_case_name = re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__).lower()
        # Add prefix from settings or use default "core_"
        prefix = settings.db_table_prefix if settings.db_table_prefix else "core"
        return f"{prefix}_{snake_case_name}"
