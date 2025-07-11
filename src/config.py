# Configurações globais e segredos (ajuste conforme necessário)

import os
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

def get_env_var(name: str, default: Optional[str] = None) -> str:
    """Obtém variável de ambiente com validação."""
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Variável de ambiente {name} não encontrada")
    return value

# Configurações do Databricks
DATABRICKS_WORKSPACE = get_env_var("DATABRICKS_WORKSPACE")
DATABRICKS_TOKEN = get_env_var("DATABRICKS_TOKEN")

# Configurações do DBFS
DBFS_BASE_PATH = "/FileStore/tables"
DBFS_RAW_PATH = f"{DBFS_BASE_PATH}/raw"
DBFS_PROCESSED_PATH = f"{DBFS_BASE_PATH}/processed"
DBFS_EXPORTS_PATH = f"{DBFS_BASE_PATH}/exports"
DBFS_LOGS_PATH = f"{DBFS_BASE_PATH}/logs"

# Configurações do Catálogo
CATALOG_NAME = "benchmarking"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

PRODUCT_TABLE_BRONZE = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.products"
PRODUCT_TABLE_SILVER = f"{CATALOG_NAME}.{SILVER_SCHEMA}.products"

# Configurações de Processamento
PROCESSING_CONFIG = {
    "embedding_model": "all-MiniLM-L6-v2",
    "similarity_threshold": 0.7,
    "batch_size": 1000
}

# Configurações de Email
EMAIL_CONFIG = {
    "enabled": True,
    "api_key": get_env_var("SENDGRID_API_KEY"),
    "from_email": get_env_var("EMAIL_FROM"),
    "to_emails": get_env_var("EMAIL_TO").split(","),
    "subject": f"Relatório de Produtos - {datetime.now().strftime('%d/%m/%Y')}"
}

# Configurações de Logging
LOGGING_CONFIG = {
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
        "max_retries": 3,
        "categories": [
            "Eletroportateis",
            "Informatica",
            "Tv e Video",
            "Moveis",
            "Eletrodomesticos",
            "Celulares"
        ]
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

# Configurações de armazenamento
STORAGE_CONFIG = {
    "raw_path": DBFS_RAW_PATH,
    "processed_path": DBFS_PROCESSED_PATH,
    "exports_path": DBFS_EXPORTS_PATH,
    "delta_tables": {
        "bemol": SOURCE_TABLE,
        "magalu_raw": MAGALU_RAW_TABLE,
        "magalu_processed": MAGALU_PROCESSED_TABLE,
        "matches": PRODUCT_MATCHES_TABLE
    }
}

# DATA STORAGE & PATHS
SOURCE_TABLE = f"{CATALOG_NAME}.feed_varejo_vtex"
MAGALU_RAW_TABLE = f"{CATALOG_NAME}.raw_magalu_products"
MAGALU_PROCESSED_TABLE = f"{CATALOG_NAME}.processed_magalu_products"
PRODUCT_MATCHES_TABLE = f"{CATALOG_NAME}.product_matches"

# SCRAPING CONFIG
BASE_URL = "http://some-ecommerce-site.com/products"
REQUESTS_TIMEOUT_SECONDS = 20
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# NLP MODEL CONFIG
SENTENCE_TRANSFORMER_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
