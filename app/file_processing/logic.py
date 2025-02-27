import os
import mimetypes
import json
from app.utils.logger import logger
from google.cloud import storage
from app.file_processing.processors import get_file_processor


def detect_file_type(object_name: str) -> str:
    """
    Detect the MIME type of the file based on its extension.
    """
    mime_type, _ = mimetypes.guess_type(object_name)
    return mime_type or "application/octet-stream"


def download_file(bucket_name: str, object_name: str) -> tuple[str, bytes]:
    """
    Download a file from a GCP bucket and return its contents as a string.

    Args:
        bucket_name (str): Name of the bucket.
        object_name (str): Name of the file in the bucket.

    Returns:
        str: Contents of the file as a string.

    Raises:
        RuntimeError: If file download fails.
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        content = blob.download_as_bytes()
        mime_type = detect_file_type(object_name)
        logger.info(
            f"Downloaded file: {bucket_name}/{object_name}, MIME type: {mime_type}"
        )
        return mime_type, content
    except Exception as e:
        logger.error(
            f"Error downloading file from gs://{bucket_name}/{object_name}: {e}"
        )
        raise RuntimeError(f"Error downloading file: {str(e)}") from e


def move_file(bucket_name: str, source_path: str, destination_path: str):
    """
    Move a file within a GCP bucket from one path to another.

    Args:
        bucket_name (str): Name of the GCP bucket.
        source_path (str): Current path of the file in the bucket.
        destination_path (str): Target path of the file in the bucket.

    Returns:
        None
    """
    try:
        # Initialize GCP Storage client
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        source_blob = bucket.blob(source_path)

        # Copy the file to the new location
        bucket.copy_blob(source_blob, bucket, destination_path)
        logger.info(f"Copied file from {source_path} to {destination_path}")

        # Delete the original file
        source_blob.delete()
        logger.info(f"Deleted original file: {source_path}")
    except Exception as e:
        logger.error(
            f"Failed to move file from {source_path} to {destination_path}: {e}"
        )
        raise


def upload_output_file(bucket_name: str, object_name: str, content: bytes, content_type=None):
    """
    Upload a file to a GCP bucket.

    Args:
        bucket_name (str): Name of the bucket.
        object_name (str): Destination file name in the bucket.
        content (bytes or str): Content of the file to upload.
        content_type (str, optional): Content type of the file.

    Returns:
        str: GCS path of the uploaded file.

    Raises:
        RuntimeError: If file upload fails.
    """
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)

        logger.info(f"Uploading file to gs://{bucket_name}/{object_name}")
        
        # Determine content type if not provided
        if content_type is None:
            content_type = detect_file_type(object_name)
        
        # Handle string vs bytes content
        if isinstance(content, str):
            blob.upload_from_string(content, content_type=content_type)
        else:
            blob.upload_from_string(content, content_type=content_type)
            
        return f"gs://{bucket_name}/{object_name}"
    except Exception as e:
        logger.error(f"Error uploading file to gs://{bucket_name}/{object_name}: {e}")
        raise RuntimeError(f"Error uploading file: {str(e)}") from e


def process_file_logic(bucket_name: str, object_name: str, db):
    """
    Core logic to process a file from a GCP bucket.

    Args:
        bucket_name (str): Name of the bucket.
        object_name (str): File path in the bucket.
        db: Database session.

    Returns:
        dict: Details of the processing.
    """
    try:
        # Download the file
        mime_type, file_content = download_file(bucket_name, object_name)

        # Determine the appropriate processor based on file type
        processor = get_file_processor(mime_type, object_name, file_content)
        
        # Set bucket info in processor context (for Mother Parkers processor)
        if hasattr(processor, 'context'):
            processor.context['bucket_name'] = bucket_name
            processor.context['file_path'] = object_name

        # Process the file and get the processed output
        processed_content = processor.process(file_content)
        logger.info(f"Processed file {object_name} successfully")

        # Prepare the processed path with `_output` appended
        base_name, ext = os.path.splitext(object_name)
        processed_path = base_name.replace("new/", "processed/") + ext
        processed_output_path = (
            base_name.replace("new/", "processed/") + "_output" + ext
        )
        
        # For Mother Parkers files, also store validation report
        validation_report_path = None
        if hasattr(processor, 'validation_report') and processor.validation_report:
            validation_report_path = base_name.replace("new/", "processed/") + "_validation.json"
            report_json = json.dumps(processor.validation_report, indent=2)
            upload_output_file(bucket_name, validation_report_path, report_json, "application/json")
            logger.info(f"Uploaded validation report to gs://{bucket_name}/{validation_report_path}")

        # Upload processed output
        upload_output_file(bucket_name, processed_output_path, processed_content)

        # Move the original file from /new to /processed
        processed_path = object_name.replace("new/", "processed/", 1)
        move_file(bucket_name, object_name, processed_path)

        result = {
            "status": "processed",
            "processed_path": processed_path,
            "processed_output_path": processed_output_path,
        }
        
        # Add validation report path if available
        if validation_report_path:
            result["validation_report_path"] = validation_report_path
            
        return result
    except Exception as e:
        logger.error(f"Failed to process file {object_name}: {e}")
        raise RuntimeError(f"Error processing file: {str(e)}")
