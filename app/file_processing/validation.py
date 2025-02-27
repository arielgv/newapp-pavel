from app.utils.logger import logger

def validate_file(file_content: bytes) -> bool:
    """
    Validate the file content.
    For example, ensure it is a valid XLS file or follows specific rules.
    """
    try:
        # Add validation logic here (e.g., check file type, schema)
        if not file_content:
            raise ValueError("File is empty")
        
        # Example validation: check for a specific marker in content
        if b"<ValidMarker>" not in file_content:
            raise ValueError("Invalid file format")
        
        logger.info("File validation passed")
        return True
    except Exception as e:
        logger.error(f"File validation failed: {e}")
        return False
