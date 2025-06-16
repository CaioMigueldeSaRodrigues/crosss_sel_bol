# Configurações globais e segredos (ajuste conforme necessário)

import os
from datetime import datetime

# Configurações do Databricks
DATABRICKS_WORKSPACE = "https://dbc-dp-1234567890123456.cloud.databricks.com"
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# Configurações do DBFS
DBFS_BASE_PATH = "/FileStore/tables"
DBFS_RAW_PATH = f"{DBFS_BASE_PATH}/raw"
DBFS_PROCESSED_PATH = f"{DBFS_BASE_PATH}/processed"
DBFS_EXPORTS_PATH = f"{DBFS_BASE_PATH}/exports"
DBFS_LOGS_PATH = f"{DBFS_BASE_PATH}/logs"

# Configurações do Catálogo
CATALOG_NAME = "bol"
SOURCE_TABLE = f"{CATALOG_NAME}.feed_varejo_vtex"
MAGALU_RAW_TABLE = f"{CATALOG_NAME}.raw_magalu_products"
MAGALU_PROCESSED_TABLE = f"{CATALOG_NAME}.processed_magalu_products"
PRODUCT_MATCHES_TABLE = f"{CATALOG_NAME}.product_matches"

# Configurações de Processamento
SIMILARITY_THRESHOLD = 0.7
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
BATCH_SIZE = 1000

# Configurações de Email
EMAIL_CONFIG = {
    "enabled": True,
    "api_key": os.getenv("SENDGRID_API_KEY"),
    "from_email": "seu-email@bemol.com.br",
    "to_emails": [
        "destinatario1@bemol.com.br",
        "destinatario2@bemol.com.br"
    ],
    "subject": f"Relatório de Produtos - {datetime.now().strftime('%d/%m/%Y')}"
}

# Configurações de Logging
LOG_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": f"{DBFS_LOGS_PATH}/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
}

# Configurações de Scraping
SCRAPING_CONFIG = {
    "magalu": {
        "base_url": "https://www.magazineluiza.com.br",
        "search_url": "https://www.magazineluiza.com.br/busca/{query}",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        },
        "timeout": 30,
        "max_retries": 3
    },
    "bemol": {
        "base_url": "https://www.bemol.com.br",
        "search_url": "https://www.bemol.com.br/busca/{query}",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        },
        "timeout": 30,
        "max_retries": 3
    }
}

# Configurações de Exportação
EXPORT_CONFIG = {
    "excel": {
        "sheet_name": "Produtos Correspondentes",
        "columns": [
            "bemol_id",
            "bemol_title",
            "bemol_price",
            "magalu_id",
            "magalu_title",
            "magalu_price",
            "similarity_score"
        ]
    }
}

# Configurações de processamento
PROCESSING_CONFIG = {
    "embedding_model": "all-MiniLM-L6-v2",
    "similarity_threshold": 0.7,
    "batch_size": 1000
}

# Configurações de armazenamento
STORAGE_CONFIG = {
    "raw_path": "/dbfs/FileStore/tables/raw",
    "processed_path": "/dbfs/FileStore/tables/processed",
    "exports_path": "/dbfs/FileStore/tables/exports",
    "delta_tables": {
        "bemol": "bol.feed_varejo_vtex",
        "magalu_raw": "bol.raw_magalu_products",
        "magalu_processed": "bol.processed_magalu_products",
        "matches": "bol.product_matches"
    }
}

# Configurações de logging
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": "/dbfs/FileStore/logs/scraping.log"
}
