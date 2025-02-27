from sqlalchemy import Column, String, Text, Integer, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db.base_class import Base
from app.core.config import settings


class Datasets(Base):
    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id = Column(String(50), unique=True, nullable=False)
    name = Column(Text, nullable=False)
    category = Column(String(100))
    reference = Column(Text)
    description = Column(Text)
    objects = relationship(
        "DatasetObjects", back_populates="dataset", cascade="all, delete-orphan"
    )
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class DatasetObjects(Base):
    id = Column(Integer, primary_key=True, autoincrement=True)
    dataset_id = Column(String(50), ForeignKey(f"{settings.db_table_prefix}_datasets.dataset_id"), nullable=False)
    file_type = Column(String(50), nullable=False)
    bucket_name = Column(String(100), nullable=False)
    object_path = Column(Text, nullable=False)
    dataset = relationship("Datasets", back_populates="objects")
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow
    )


# Example Insertion
"""
new_dataset = Dataset(
    dataset_id="dataset_001",
    name="Production Places with Deforestation",
    category="Deforestation",
    reference="Supplier A - Colombia - November 2024",
    description="This dataset contains data of production places identified with deforestation risks."
)

new_objects = [
    DatasetObject(file_type="CSV", download_url="https://storage-api-dev-764527084184.europe-west1.run.app/datasets/production_places_deforestation.csv"),
    DatasetObject(file_type="GeoJSON", download_url="https://storage-api-dev-764527084184.europe-west1.run.app/datasets/production_places_deforestation.geojson")
]

new_dataset.objects.extend(new_objects)

session.add(new_dataset)
session.commit()
"""
