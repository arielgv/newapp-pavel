import re
import pandas as pd
import uuid
from datetime import datetime
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
from app.utils.logger import logger
from app.file_processing.mother_parkers.models import (
    Entity, Country, Engagement, SaleTransaction, 
    SaleTransactionParam, EngagementEntity, EntityClient, CosaParam
)

class DBOperations:
    def __init__(self, connection_string, use_db=True, single_record_mode=False):
        """
        Inicializa las operaciones de base de datos para Mother Parkers.
        
        Args:
            connection_string: Cadena de conexión a la base de datos
            use_db: Si es True, realiza cambios reales en la BD. Si es False, simula las operaciones.
            single_record_mode: Si es True, procesa solo un registro por tipo.
        """
        self.use_db = use_db
        self.single_record_mode = single_record_mode
        
        if self.use_db:
            try:
                self.engine = create_engine(connection_string)
                self.Session = sessionmaker(bind=self.engine)
                logger.info("Conexión a la base de datos establecida correctamente")
            except Exception as e:
                logger.error(f"Error al conectar con la base de datos: {e}")
                self.use_db = False
        
        self.client_id = 1  #  Mother Parkers
        self.engagement_id = 1  # Engagement default Hardcoded
        self.transactions_to_process = 0
        self.transactions_processed = 0
        self.param_mapping = {}  

    def normalize_container_number(self, number):
        #sin espacios y todo en mayusculas
        if number is None:
            return ''
        return re.sub(r'\s', '', str(number).upper())

    def format_country_name(self, country):
        """Country formatting"""
        if not country:
            return None
        # Convierte a minúsculas y luego capitaliza cada palabra
        return ' '.join(word.capitalize() for word in str(country).lower().split())

    def find_matching_row(self, container_number, worksheet):
        """Busca una fila con un número de contenedor coincidente en una hoja de trabajo."""
        normalized_container = self.normalize_container_number(container_number)
        for row in worksheet.iter_rows(min_row=2, values_only=True):
            if self.normalize_container_number(row[11]) == normalized_container:  # Column L is index 11
                return row
        return None

    def process_workbook(self, workbook):
        """
        Procesa el libro de Excel completo, extrayendo entidades y transacciones.
        
        Args:
            workbook: Objeto de libro de Excel (Workbook) de openpyxl
            
        Returns:
            dict: Resultados del procesamiento
        """
        results = {
            "entities_processed": 0,
            "transactions_processed": 0,
            "errors": []
        }
        
        try:
            #  entidades
            entities_count = self.process_entities_from_workbook(workbook)
            results["entities_processed"] = entities_count
            
            #  transacciones
            transactions_count = self.process_transactions_from_workbook(workbook)
            results["transactions_processed"] = transactions_count
            
            logger.info(f"Procesamiento completo: {entities_count} entidades, {transactions_count} transacciones")
            return results
            
        except Exception as e:
            error_msg = f"Error al procesar el libro Excel: {str(e)}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
            return results

    def process_entities_from_workbook(self, workbook):
        """
        Procesa las entidades directamente desde un objeto Workbook de openpyxl.
        
        Args:
            workbook: Objeto de libro Excel (openpyxl.Workbook)
            
        Returns:
            int: Número de entidades procesadas
        """
        logger.info("Iniciando procesamiento de entidades desde workbook...")
        processed_count = 0
        
        try:
            sheets_to_process = ['Database - Others', 'Database-RA+FT Coop', 'Single Supplier Table']
            for sheet_name in sheets_to_process:
                if sheet_name not in workbook.sheetnames:
                    logger.warning(f"La hoja '{sheet_name}' no existe en el workbook")
                    continue
                
                sheet = workbook[sheet_name]
                
                # Encontrar la fila del encabezado
                header_row = None
                company_name_col = None
                
                # se busca la columna "Company Name" en las primeras 5 filas
                for row_idx in range(1, 6):
                    for col_idx, cell in enumerate(sheet[row_idx], 1):
                        if cell.value == "Company Name":
                            header_row = row_idx
                            company_name_col = col_idx
                            break
                    if header_row:
                        break
                
                if not header_row:
                    logger.warning(f"No se encontró la columna 'Company Name' en la hoja '{sheet_name}'")
                    continue
                
                # mapeo de columnas a sus índices
                columns = {}
                for col_idx, cell in enumerate(sheet[header_row], 1):
                    if cell.value:
                        columns[cell.value] = col_idx
                
                # Procesar las filas de datos
                entities_processed_in_sheet = 0
                for row_idx in range(header_row + 1, sheet.max_row + 1):
                    
                    company_name_cell = sheet.cell(row=row_idx, column=company_name_col)
                    if not company_name_cell.value:
                        continue
                    
                    # diccionario con los datos de la entidad
                    entity_data = {"Company Name": company_name_cell.value}
                    for column_name, col_idx in columns.items():
                        entity_data[column_name] = sheet.cell(row=row_idx, column=col_idx).value
                    
                    
                    entity_id = self.create_or_update_entity(entity_data)
                    if entity_id:
                        processed_count += 1
                        entities_processed_in_sheet += 1
                        logger.info(f"Entidad creada/actualizada en '{sheet_name}': {entity_data['Company Name']} (ID: {entity_id})")
                        
                        if self.single_record_mode and entities_processed_in_sheet > 0:
                            logger.info(f"Modo registro único: Se ha procesado 1 entidad de la hoja '{sheet_name}'")
                            break
                
                logger.info(f"Procesadas {entities_processed_in_sheet} entidades de la hoja '{sheet_name}'")
            
            logger.info(f"Procesamiento de entidades completado. Total: {processed_count}")
            return processed_count
        except Exception as e:
            logger.error(f"Error general en process_entities_from_workbook: {e}")
            return 0

    def create_or_update_entity(self, entity_data):
        """
        Crea o actualiza una entidad en la base de datos.
        
        Args:
            entity_data: Diccionario con los datos de la entidad
            
        Returns:
            int: ID de la entidad creada o actualizada, o None si falla
        """
        if not self.use_db:
            logger.info(f"Modo simulación: Se procesaría la entidad '{entity_data.get('Company Name')}'")
            return None
            
        entity_name = entity_data.get('Company Name')
        if not entity_name:
            logger.warning("Se intentó crear una entidad sin nombre")
            return None
            
        session = self.Session()
        try:
            entity = session.query(Entity).filter_by(entityname=entity_name).first()
            
            if not entity:
                logger.info(f"Creando nueva entidad: {entity_name}")
                
                # Obtener el próximo ID de entidad
                max_id = session.query(func.max(Entity.entityid)).scalar() or 0
                new_entity_id = max_id + 1
                
                # Formatear el nombre del país
                country_name = self.format_country_name(entity_data.get('Country'))
                country_id = self.get_country_id(country_name, session) if country_name else None
                
                entity = Entity(
                    entityid=new_entity_id,
                    entityname=entity_name,
                    entitycontactmail=entity_data.get('Email'),
                    entitylatitude=entity_data.get('Latitude'),
                    entitylongitude=entity_data.get('Longitude'),
                    entityphonenumber=entity_data.get('Phone'),
                    entitymobilenumber=entity_data.get('Whatsapp'),
                    entityaddress=entity_data.get('Address'),
                    entitycreateddate=datetime.now(),
                    entitycreateduser='system',
                    entitylastmodifieddate=datetime.now(),
                    entitylastmodifieduser='system',
                    countryid=country_id,
                    entitystateprovince=entity_data.get('Province/State'),
                    entityenabled=True,
                    entityduplicated=False,
                    entityzipcode=entity_data.get('Postal/Zip Code')
                )
                session.add(entity)
                session.flush()  # getting entityid

                # EngagementEntity
                engagement_entity = EngagementEntity(engagementid=self.engagement_id, entityid=entity.entityid)
                session.add(engagement_entity)

                # EntityClient
                entity_client = EntityClient(entityid=entity.entityid, clientid=self.client_id)
                session.add(entity_client)
                
                logger.info(f"Entidad creada con ID: {entity.entityid}")
            else:
                logger.info(f"Entidad ya existe: {entity_name} (ID: {entity.entityid})")

            session.commit()
            return entity.entityid if entity else None
        except Exception as e:
            session.rollback()
            logger.error(f"Error al procesar entidad '{entity_name}': {e}")
            return None
        finally:
            session.close()

    def process_transactions_from_workbook(self, workbook):
        """
        Procesa las transacciones directamente desde un objeto Workbook de openpyxl.
        
        Args:
            workbook: Objeto de libro Excel (openpyxl.Workbook)
            
        Returns:
            int: Número de transacciones procesadas
        """
        logger.info("Iniciando procesamiento de transacciones desde workbook...")
        processed_count = 0
        
        if not self.use_db:
            logger.info("Modo simulación: No se realizarán transacciones en la base de datos")
            return 0
            
        try:
            # CONTROL:  si las hojas necesarias existen
            required_sheets = ["Manual Sheet", "Worksheet- Coffee", "Worksheet- Tea"]
            for sheet_name in required_sheets:
                if sheet_name not in workbook.sheetnames:
                    logger.error(f"Hoja requerida '{sheet_name}' no encontrada")
                    return 0
            
            
            manual_sheet = workbook["Manual Sheet"]
            coffee_sheet = workbook["Worksheet- Coffee"]
            tea_sheet = workbook["Worksheet- Tea"]
            
            #filas y columnas de encabezado
            manual_header_row = self.find_header_row(manual_sheet, ["Exporter Name", "Container Number"])
            if not manual_header_row:
                logger.error("No se encontraron las columnas necesarias en 'Manual Sheet'")
                return 0
            
            #mapeo de nombres de columna a índices
            manual_columns = {}
            for col_idx, cell in enumerate(manual_sheet[manual_header_row], 1):
                if cell.value:
                    manual_columns[cell.value] = col_idx
            
            #  número total de transacciones a procesar
            self.transactions_to_process = 0
            for row_idx in range(manual_header_row + 1, manual_sheet.max_row + 1):
                exporter_name = manual_sheet.cell(row=row_idx, column=manual_columns.get("Exporter Name", 0)).value
                if exporter_name and str(exporter_name).strip():
                    self.transactions_to_process += 2  # Dos transacciones por fila válida
            
            logger.info(f"Se procesarán hasta {self.transactions_to_process} transacciones (2 por fila válida).")
            
            # Procesar cada fila en Manual Sheet
            for row_idx in range(manual_header_row + 1, manual_sheet.max_row + 1):
                # control: nombre de exportador
                exporter_name = manual_sheet.cell(row=row_idx, column=manual_columns.get("Exporter Name", 0)).value
                if not exporter_name or not str(exporter_name).strip():
                    continue
                
                # diccionario con los datos de la transacción
                transaction_data = {}
                for column_name, col_idx in manual_columns.items():
                    transaction_data[column_name] = manual_sheet.cell(row=row_idx, column=col_idx).value
                
                # la fila es válida? (no tiene celdas rojas)
                if self.is_valid_row(manual_sheet, row_idx):
                    logger.info(f"Procesando fila {row_idx}: Exportador='{exporter_name}', Contenedor='{transaction_data.get('Container Number')}'")
                    
                    # Primera transacción con datos originales
                    trans_id = self.create_transaction(transaction_data, is_second_transaction=False)
                    processed_count += 1
                    
                    if trans_id:
                        logger.info(f"Transacción primaria creada: ID {trans_id}")
                    
                    # se busca  match en Coffee o Tea para la segunda transacción
                    container_number = transaction_data.get('Container Number')
                    
                    if container_number and pd.notna(container_number):
                        #  Coffee
                        vendor = None
                        sheet_name = None
                        
                        
                        coffee_match = self.find_matching_container(coffee_sheet, container_number)
                        if coffee_match:
                            vendor = coffee_match.get("Vendor")
                            sheet_name = "Coffee"
                        
                        # si no se encuentra en Coffee,  se busca en Tea
                        if not vendor:
                            tea_match = self.find_matching_container(tea_sheet, container_number)
                            if tea_match:
                                vendor = tea_match.get("Vendor")
                                sheet_name = "Tea"
                        
                        if vendor:
                            # Segunda transacción con vendor de Coffee/Tea
                            trans_id = self.create_transaction(transaction_data, is_second_transaction=True, vendor=vendor)
                            processed_count += 1
                            
                            if trans_id:
                                logger.info(f"Transacción secundaria creada: ID {trans_id} - Vendor: {vendor} (de hoja {sheet_name})")
                        else:
                            logger.warning(f"No se encontró match para el contenedor {container_number} en las hojas de Coffee o Tea.")
                    else:
                        logger.warning(f"Fila {row_idx}: Número de contenedor vacío o no válido")
                    
                    if self.single_record_mode and processed_count > 0:
                        logger.info(f"Modo registro único: Se ha procesado {processed_count} transacción(es)")
                        break
                else:
                    logger.info(f"Fila {row_idx} invalidada: Tiene celdas marcadas en rojo")
            
            logger.info(f"Se procesaron {processed_count} transacciones, de las cuales {self.transactions_processed} se inscribieron en la BD.")
            if self.transactions_processed != processed_count:
                logger.warning(f"{processed_count - self.transactions_processed} transacciones no se pudieron procesar.")
            
            return processed_count
        except Exception as e:
            logger.error(f"Error general en process_transactions_from_workbook: {e}")
            return 0

    def find_header_row(self, sheet, required_columns):
        """
        Busca la fila de encabezado que contiene las columnas requeridas.
        
        Args:
            sheet: Hoja de trabajo de openpyxl
            required_columns: Lista de nombres de columnas requeridas
            
        Returns:
            int: Número de fila del encabezado, o None si no se encuentra
        """
        # Buscar en las primeras 5 filas
        for row_idx in range(1, 6):
            col_values = [cell.value for cell in sheet[row_idx]]
            
            # Verificar si todas las columnas requeridas están presentes
            if all(col in col_values for col in required_columns):
                return row_idx
                
        return None

    def find_matching_container(self, sheet, container_number):
        """
        Busca un número de contenedor en una hoja y devuelve la fila correspondiente.
        
        Args:
            sheet: Hoja de trabajo de openpyxl
            container_number: Número de contenedor a buscar
            
        Returns:
            dict: Datos de la fila encontrada, o None si no se encuentra
        """
        # Normalizar el número de contenedor de búsqueda
        normalized_container = self.normalize_container_number(container_number)
        
        # Encontrar la fila de encabezado y la columna de número de contenedor
        header_row = self.find_header_row(sheet, ["Container #"])
        if not header_row:
            return None
            
        # Crear un mapeo de nombres de columna a índices
        columns = {}
        for col_idx, cell in enumerate(sheet[header_row], 1):
            if cell.value:
                columns[cell.value] = col_idx
        
        container_col_idx = columns.get("Container #")
        if not container_col_idx:
            return None
            
        # Buscar en todas las filas
        for row_idx in range(header_row + 1, sheet.max_row + 1):
            cell_value = sheet.cell(row=row_idx, column=container_col_idx).value
            if cell_value and self.normalize_container_number(cell_value) == normalized_container:
                # Crear un diccionario con los datos de la fila
                row_data = {}
                for column_name, col_idx in columns.items():
                    row_data[column_name] = sheet.cell(row=row_idx, column=col_idx).value
                return row_data
                
        return None

    def is_valid_row(self, sheet, row_idx):
        """
        Verifica si una fila es válida (no tiene celdas marcadas en rojo).
        
        Args:
            sheet: Hoja de trabajo de openpyxl
            row_idx: Índice de fila a verificar
            
        Returns:
            bool: True si la fila es válida, False en caso contrario
        """
        try:
            # Verificar columnas C, D y E (índices 3, 4, 5 en base 1)
            for col_idx in [3, 4, 5]:
                cell = sheet.cell(row=row_idx, column=col_idx)
                if cell.fill.start_color.index == "FF0000":  # Rojo
                    return False
            return True
        except Exception as e:
            logger.error(f"Error al validar fila {row_idx}: {e}")
            return False

    def create_transaction(self, transaction_data, is_second_transaction=False, vendor=None):
        """
        Crea una transacción en la base de datos.
        
        Args:
            transaction_data: Diccionario con los datos de la transacción
            is_second_transaction: Indica si es la segunda transacción
            vendor: Nombre del vendor para la segunda transacción
            
        Returns:
            int: ID de la transacción creada, o None si falla
        """
        if not self.use_db:
            return None
            
        session = self.Session()
        try:
            # Verificar que la entidad de origen existe
            from_entity_name = transaction_data.get('Exporter Name')
            if not from_entity_name:
                logger.warning("No se pudo crear transacción: Falta el nombre del exportador")
                return None
                
            from_entity_id = self.get_entity_id(from_entity_name, session)
            if not from_entity_id:
                logger.warning(f"No se pudo crear transacción: Entidad de origen '{from_entity_name}' no encontrada")
                return None
            
            # Obtener el próximo ID de transacción
            max_trans_id = session.query(func.max(SaleTransaction.saletransactionid)
                                        ).filter_by(clientid=self.client_id).scalar() or 0
            new_trans_id = max_trans_id + 1
            
            transaction = SaleTransaction(
                clientid=self.client_id,
                saletransactionid=new_trans_id,
                saletransactionentityfromid=from_entity_id,
                saletransactionentitytoid=self.get_default_to_entity_id(session),
                engagementid=self.engagement_id,
                saletransactioncreateddate=datetime.now(),
                saletransactioncreateduser='system',
                saletransactionlastmodifieddat=datetime.now(),
                saletransactionlastmodifieduse='system',
                saletransactionparentclientid=None,
                saletransactionparentid=None
            )
            session.add(transaction)
            session.flush()

            # Procesar parámetros
            params_added = 0
            for column, value in transaction_data.items():
                if pd.isna(value):
                    continue
                    
                if is_second_transaction and column == 'Mill Name' and vendor:
                    value = vendor  # Usar el vendor de Coffee/Tea sheet para la segunda transacción
                
                cosaparam_id = self.get_cosaparam_id(column, session)
                if cosaparam_id:
                    param = SaleTransactionParam(
                        clientid=self.client_id,
                        saletransactionid=transaction.saletransactionid,
                        cosaparamid=cosaparam_id,
                        saletransactionparamvalue=str(value)
                    )
                    session.add(param)
                    params_added += 1
                else:
                    logger.debug(f"No se encontró CosaParam para la columna: {column}")

            session.commit()
            self.transactions_processed += 1
            logger.info(f"Transacción {'secundaria' if is_second_transaction else 'primaria'} creada con {params_added} parámetros")
            return transaction.saletransactionid
        except Exception as e:
            session.rollback()
            logger.error(f"Error al crear transacción: {e}")
            return None
        finally:
            session.close()

    def get_entity_id(self, entity_name, session=None):
        """
        Busca el ID de una entidad por nombre.
        
        Args:
            entity_name: Nombre de la entidad
            session: Sesión de SQLAlchemy (opcional)
            
        Returns:
            int: ID de la entidad, o None si no se encuentra
        """
        if not self.use_db or not entity_name:
            return None
            
        close_session = False
        if not session:
            session = self.Session()
            close_session = True
            
        try:
            entity = session.query(Entity).filter_by(entityname=entity_name).first()
            return entity.entityid if entity else None
        except Exception as e:
            logger.error(f"Error al buscar entidad '{entity_name}': {e}")
            return None
        finally:
            if close_session:
                session.close()

    def get_default_to_entity_id(self, session=None):
        """
        Obtiene el ID de la entidad de destino predeterminada.
        
        Args:
            session: Sesión de SQLAlchemy (opcional)
            
        Returns:
            int: ID de la entidad predeterminada
        """
        if not self.use_db:
            return 1
            
        # En este caso es un valor fijo, pero podrías implementar lógica para buscar
        # la entidad de destino correcta según algún criterio
        default_id = 1
        logger.debug(f"Usando entidad de destino predeterminada con ID: {default_id}")
        return default_id

    def get_country_id(self, country_name, session=None):
        """
        Busca el ID del país por nombre.
        
        Args:
            country_name: Nombre del país
            session: Sesión de SQLAlchemy (opcional)
            
        Returns:
            int: ID del país, o None si no se encuentra
        """
        if not self.use_db or not country_name:
            return None
            
        close_session = False
        if not session:
            session = self.Session()
            close_session = True
            
        try:
            country = session.query(Country).filter_by(countryname=country_name).first()
            if country:
                return country.countryid
            else:
                logger.warning(f"País no encontrado: {country_name}")
                return None
        except Exception as e:
            logger.error(f"Error al buscar país '{country_name}': {e}")
            return None
        finally:
            if close_session:
                session.close()

    def get_cosaparam_id(self, param_name, session=None):
        """
        Busca el ID del parámetro por nombre.
        
        Args:
            param_name: Nombre del parámetro
            session: Sesión de SQLAlchemy (opcional)
            
        Returns:
            int: ID del parámetro, o None si no se encuentra
        """
        if not self.use_db or not param_name:
            return None
            
        # Usar caché para evitar consultas repetidas
        if param_name in self.param_mapping:
            return self.param_mapping[param_name]
            
        close_session = False
        if not session:
            session = self.Session()
            close_session = True
            
        try:
            # Intentar buscar coincidencia exacta
            cosaparam = session.query(CosaParam).filter_by(cosaparamname=param_name).first()
            
            # Si no encuentra, intentar buscar de forma más flexible
            if not cosaparam:
                # Buscar ignorando mayúsculas/minúsculas y espacios
                norm_param_name = param_name.lower().replace(' ', '')
                for param in session.query(CosaParam).all():
                    norm_db_name = param.cosaparamname.lower().replace(' ', '')
                    if norm_param_name == norm_db_name:
                        cosaparam = param
                        break
            
            if cosaparam:
                self.param_mapping[param_name] = cosaparam.cosaparamid
                return cosaparam.cosaparamid
            else:
                logger.warning(f"Parámetro no encontrado: {param_name}")
                self.param_mapping[param_name] = None
                return None
        except Exception as e:
            logger.error(f"Error al buscar parámetro '{param_name}': {e}")
            return None
        finally:
            if close_session:
                session.close()