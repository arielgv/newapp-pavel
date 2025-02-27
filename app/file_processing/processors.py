from abc import ABC, abstractmethod
from app.utils.logger import logger
from app.file_processing.excel_validation.loader import is_excel_file, is_mother_parkers_format

def get_file_processor(mime_type: str, file_name: str, file_content: bytes):
    """
    Determine the appropriate processor for the given file.

    Args:
        mime_type (str): The MIME type of the file (e.g., "text/csv" or "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet").
        file_name (str): The name of the file.
        file_content (bytes): The content of the file.

    Returns:
        Processor: An instance of the selected processor.
    """
    try:
        # First check if this is a Mother Parkers Excel file
        if is_excel_file(file_name) and mime_type in ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/octet-stream"]:
            if is_mother_parkers_format(file_content):
                # Import here to avoid circular imports
                from app.file_processing.processors_mother_parkers import MotherParkersExcelProcessor
                logger.info("Processing as Mother Parkers Excel file")
                processor = MotherParkersExcelProcessor()
                # Set file context for the processor
                processor.context = {
                    'bucket_name': None,  # Will be set in logic.py
                    'file_path': file_name
                }
                return processor

        # Regular file processing logic
        if mime_type in ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/octet-stream"]:
            logger.info("Processing as regular Excel file")
            return XLSXProcessor()
        elif mime_type in ["text/csv"]:
            logger.info("Processing as CSV file")
            decoded_content = file_content.decode("utf-8")  # Adjust encoding if needed
            return CSVProcessor()
        elif mime_type in ["application/json"]:
            logger.info("Processing as JSON file")
            return JSONProcessor()
        else:
            raise ValueError(f"Unsupported file type: {mime_type}")
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise RuntimeError(f"Error processing file: {str(e)}") from e


class FileProcessor(ABC):
    def __init__(self):
        self.context = {}
        self.validation_report = None
        
    @abstractmethod
    def process(self, file_content: bytes) -> bytes:
        """
        Process the file content and return the processed output.

        Args:
            file_content (bytes): The raw content of the file.

        Returns:
            bytes: The processed content.
        """
        pass

class JSONProcessor(FileProcessor):
    def process(self, file_content: bytes) -> bytes:
        # Example processing logic for JSON files
        # For demonstration, this just returns the same content
        return file_content

class CSVProcessor(FileProcessor):
    def process(self, file_content: bytes) -> bytes:
        # Example processing logic for CSV files
        # For demonstration, this just returns the same content
        return file_content

class XLSXProcessor(FileProcessor):
    def process(self, file_content: bytes) -> bytes:
        # Example processing logic for Excel files
        # For demonstration, this just returns the same content
        return file_content