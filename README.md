### NUEVAS DEPENDENCIAS:

pip install openpyxl pandas


- Reemplazar la carpeta *app* con esta otra, el funcionamiento que tenía no se ha modificado, solamente la detección del archivo entrante. 
NUEVA ESTRUCTURA:

```
cosa-core-engine/
└── app/
    ├── file_processing/
    │   ├── processors.py (modificado)
    │   ├── processors_mother_parkers.py (nuevo)
    │   ├── logic.py (modificado)
    │   ├── excel_validation/ (nuevo directorio)
    │   │   ├── __init__.py
    │   │   ├── validator.py
    │   │   └── loader.py
    │   └── mother_parkers/ (nuevo directorio)
    │       ├── __init__.py
    │       ├── models.py
    │       └── db_operations.py

```
