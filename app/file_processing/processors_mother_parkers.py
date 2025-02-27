from app.file_processing.processors import FileProcessor
from app.utils.logger import logger
from app.file_processing.excel_validation.validator import ExcelValidator
from app.file_processing.excel_validation.loader import backup_file_to_gcs, is_mother_parkers_format
from app.file_processing.mother_parkers.db_operations import DBOperations
import urllib.parse
import os
from app.core.config import settings

class MotherParkersExcelProcessor(FileProcessor):

    
    def process(self, file_content: bytes) -> bytes:

        logger.info("Procesando archivo Excel de Mother Parkers")
        
        try:
            # backup of the original file
            bucket_name = self.context.get('bucket_name', 'default-bucket')
            file_path = self.context.get('file_path', 'unknown-file.xlsx')
            backup_file_to_gcs(bucket_name, file_path, file_content)
            
            # Validate the Excel file
            validator = ExcelValidator()
            processed_content, validation_report, workbook = validator.validate_workbook_bytes(file_content)
            
            # Log validation 
            valid_rows = validation_report['stats']['valid_rows']
            total_rows = validation_report['stats']['total_rows']
            logger.info(f"Validación completada: {valid_rows}/{total_rows} filas válidas")
            
            # Store validation report as metadata
            self.validation_report = validation_report
            
            
            if valid_rows > 0:
                self.process_database_operations(workbook, validation_report)
            
            return processed_content
            
        except Exception as e:
            logger.error(f"Error al procesar archivo Excel de Mother Parkers: {e}")
            raise RuntimeError(f"Error al procesar archivo Excel de Mother Parkers: {str(e)}")

    def process_database_operations(self, workbook, validation_report):
        """
        
        
        Args:
            workbook: Openpyxl workbook object
            validation_report: Dictionary with validation results
        """
        try:
            # Get database connection string from environment variables
            encoded_password = urllib.parse.quote_plus(settings.db_password)
            db_url = f"postgresql://{settings.db_user}:{encoded_password}@{settings.db_host}:{settings.db_port}/{settings.db_name}"
            
            logger.info("Iniciando operaciones de base de datos para Mother Parkers")
            
            
            db_ops = DBOperations(db_url, use_db=True, single_record_mode=False)
            
            
            results = db_ops.process_workbook(workbook)
            
            
            logger.info(f"Resultados de procesamiento de BD: {results['entities_processed']} entidades, {results['transactions_processed']} transacciones")
            
            
            self.validation_report["database_results"] = {
                "entities_processed": results["entities_processed"],
                "transactions_processed": results["transactions_processed"],
                "errors": results["errors"]
            }
            
        except Exception as e:
            logger.error(f"Error en operaciones de base de datos: {e}")
            self.validation_report["database_results"] = {
                "error": str(e),
                "entities_processed": 0,
                "transactions_processed": 0
            }