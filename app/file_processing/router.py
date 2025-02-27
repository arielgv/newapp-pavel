import base64
import json
import os
from fastapi import APIRouter, HTTPException, Depends, Path
from sqlalchemy.orm import Session
from sqlalchemy.future import select
from sqlalchemy import update
from app.db.session import DatabaseManager
from app.db.models.file_tasks import FileTasks, ProcessingStatus
from app.db.models.datasets import Datasets, DatasetObjects
from app.file_processing.logic import process_file_logic
from app.utils.logger import logger
from asyncio import CancelledError
from datetime import timedelta
from google.cloud import storage
from google.oauth2 import service_account

router = APIRouter()


# Decode and authenticate
def get_storage_client():
    encoded_key = os.getenv("SERVICE_ACCOUNT_INFO_JSON_BASE64")
    if not encoded_key:
        raise ValueError("SERVICE_ACCOUNT_INFO_JSON_BASE64 is not set")

    service_account_info = json.loads(base64.b64decode(encoded_key))
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info
    )

    return storage.Client(credentials=credentials)


@router.get("/list-datasets/{storage_path:path}")
async def list_datasets(
    storage_path: str = Path(
        ..., description="Storage path including bucket name and optional directory"
    ),
    category: str = "",
    page: int = 1,
    page_size: int = 10,
    db: Session = Depends(DatabaseManager.get_db),
):
    bucket_parts = storage_path.split("/", 1)
    bucket_name = bucket_parts[0]
    directory = bucket_parts[1] if len(bucket_parts) > 1 else ""
    prefix = f"{directory}/processed" if directory else "processed"

    # Query the database
    datasets_query = (
        db.query(Datasets)
        .join(DatasetObjects)
        .filter(
            DatasetObjects.bucket_name == bucket_name,
            DatasetObjects.object_path.like(f"{prefix}%"),
        )
    )

    if category:
        datasets_query = datasets_query.filter(Datasets.category == category)

    total_datasets = datasets_query.count()
    datasets = datasets_query.offset((page - 1) * page_size).limit(page_size).all()

    # Build the response
    result = []
    for dataset in datasets:
        signed_urls = [
            {
                "file_type": obj.file_type,
                "download_url": get_storage_client()
                .bucket(obj.bucket_name)
                .blob(obj.object_path)
                .generate_signed_url(expiration=timedelta(minutes=10), version="v4"),
            }
            for obj in dataset.objects
        ]
        result.append(
            {
                "dataset_id": dataset.dataset_id,
                "name": dataset.name,
                "category": dataset.category,
                "reference": dataset.reference,
                "description": dataset.description,
                "objects": signed_urls,
            }
        )

    return {
        "category": category,
        "page": page,
        "page_size": page_size,
        "total": total_datasets,
        "datasets": result,
    }


@router.get("/status/{file_id}")
def get_task_status(file_id: str, db: Session = Depends(DatabaseManager.get_db)):
    """
    Get the status of a file processing task.
    """
    try:
        logger.info(f"Fetching status for file_id: {file_id}")
        result = db.execute(select(FileTasks).filter(FileTasks.file_id == file_id))
        task = result.scalars().first()
        if not task:
            raise HTTPException(
                status_code=404, detail=f"No task found for file_id: {file_id}"
            )
        logger.info(f"Task found for file_id: {file_id} with status: {task.status}")
        return {
            "file_id": task.file_id,
            "status": task.status,
            "file_path": task.file_path,
            "bucket": task.bucket,
            "processors": task.processors,
            "validation_csv_path": task.processed_output_path,
            "processed_output_path": task.processed_output_path,
        }
    except Exception as e:
        logger.error(f"Failed to fetch task status for file_id {file_id}: {e}")
        raise HTTPException(status_code=500, detail="Error fetching task status")


@router.post("/files/process-file")
def process_file(event_data: dict, db: Session = Depends(DatabaseManager.get_db)):
    """
    Triggered when a file is uploaded in the bucket. Process the file.

    Args:
        event_data (dict): Event payload containing bucket and object names.
        db: Database session.

    Returns:
        dict: Status and details of the processing.
    """
    logger.info(f"Received event: {event_data}")
    event_data = json.loads(
        base64.b64decode(event_data["message"]["data"]).decode("utf-8")
    )
    logger.info(f"Decoded event: {event_data}")
    bucket_name = event_data.get("bucket")
    object_name = event_data.get("name")

    if not bucket_name or not object_name:
        logger.error("Bucket or object name not found in event data")
        raise HTTPException(status_code=200, detail="Bucket or object name not found")

    if not object_name.startswith("new/") or object_name.rstrip("/") == "new":
        logger.info("Invalid file path, processing skipped")
        raise HTTPException(
            status_code=200, detail="Invalid file path, processing skipped"
        )

    file_base_name = object_name.split("/")[-1]
    file_name, file_extension = file_base_name.rsplit(".", 1)
    file_id = file_name

    # Check if task already exists
    result = db.execute(select(FileTasks).filter(FileTasks.file_path == object_name))
    existing_task = result.scalars().first()

    if existing_task:
        logger.info("Task already exists for file path: %s", object_name)
        raise HTTPException(status_code=200, detail="Task already exists")

    try:
        # Create a new task with PENDING status
        new_task = FileTasks(
            file_id=file_id,
            file_path=object_name,
            bucket=bucket_name,
            processors=[],
            processed_output_path=None,
            status=ProcessingStatus.PENDING,
        )
        db.add(new_task)
        db.commit()

        # Call the processing logic
        result = process_file_logic(bucket_name, object_name, db)
        new_task.processed_output_path = (
            f"gs://{bucket_name}/{result['processed_output_path']}"
        )
        new_task.status = ProcessingStatus.PROCESSED
        db.commit()

        # Update task status to PROCESSED
        stmt = (
            update(FileTasks)
            .where(FileTasks.file_id == file_id)
            .values(
                status=ProcessingStatus.PROCESSED, processors=result.get("processors")
            )
        )
        db.execute(stmt)
        db.commit()

        return {"status": "success", "details": result}
    except CancelledError:
        logger.error("Request was cancelled")
        raise
    except Exception as e:
        logger.error("Error processing file: %s", e)
        # Update task status to FAILED
        stmt = (
            update(FileTasks)
            .where(FileTasks.file_id == file_id)
            .values(status=ProcessingStatus.FAILED)
        )
        db.execute(stmt)
        db.commit()
        raise HTTPException(status_code=200, detail="Failed to process file")
