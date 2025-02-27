from sqlalchemy import Column, String, Integer, DateTime, JSON, Enum
from sqlalchemy.ext.declarative import declarative_base
from app.db.base_class import Base
import datetime
from enum import Enum as PyEnum


class ProcessingStatus(str, PyEnum):
    """Enum for the processing status of the task"""

    PENDING = "PENDING"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"

    def __str__(self):
        return self.value


class FileTasks(Base):
    id = Column(Integer, primary_key=True, index=True)
    file_id = Column(String, unique=True, nullable=False)
    file_path = Column(String, nullable=False)
    bucket = Column(String, nullable=False)
    processors = Column(JSON, nullable=False)
    processed_output_path = Column(String, nullable=True)
    status = Column(
        Enum(ProcessingStatus), nullable=False, default=ProcessingStatus.PENDING
    )

    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(
        DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow
    )
