# Configurações globais e segredos (ajuste conforme necessário)

DATABRICKS_HOST = "adb-926216925051160.0.azuredatabricks.net"
DATABRICKS_HTTP_PATH = "/sql/1.0/warehouses/ead9637a3263a02e"
DATABRICKS_TOKEN = "dapi33029ae4a4617126aef5217bccc760a6 "

SENDGRID_API_KEY = "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"
EMAIL_FROM = "caiomiguel@bemol.com.br"
EMAIL_TO = ["renatobolf@bemol.com.br"]
EMAIL_BCC = ["caiomiguel@bemol.com.br"]

# Configurações do Databricks
DATABRICKS_CONFIG = {
    "host": "adb-926216925051160.0.azuredatabricks.net",
    "http_path": "/sql/1.0/warehouses/ead9637a3263a02e",
    "token": "dapi33029ae4a4617126aef5217bccc760a6",
    "catalog": "hive_metastore",
    "schema": "bronze"
}

# Configurações do Spark
SPARK_CONFIG = {
    "app_name": "Scraping Benchmarking",
    "master": "local[*]",
    "configs": {
        "spark.sql.warehouse.dir": "/dbfs/FileStore/tables/warehouse",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true"
    }
}

# Configurações de scraping
SCRAPING_CONFIG = {
    "magalu": {
        "base_url": "https://www.magazineluiza.com.br",
        "categories": {
            "Eletroportateis": "https://www.magazineluiza.com.br/eletroportateis/l/ep/?page={}",
            "Informatica": "https://www.magazineluiza.com.br/informatica/l/in/?page={}",
            "Tv e Video": "https://www.magazineluiza.com.br/tv-e-video/l/et/?page={}",
            "Moveis": "https://www.magazineluiza.com.br/moveis/l/mo/?page={}",
            "Eletrodomesticos": "https://www.magazineluiza.com.br/eletrodomesticos/l/ed/?page={}",
            "Celulares": "https://www.magazineluiza.com.br/celulares-e-smartphones/l/te/?page={}"
        },
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
    },
    "bemol": {
        "base_url": "https://www.bemol.com.br",
        "headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
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
    "exports_path": "/dbfs/FileStore/tables/exports"
}

# Configurações de email
EMAIL_CONFIG = {
    "enabled": True,
    "sendgrid_api_key": "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",
    "sender_email": "caiomiguel@bemol.com.br",
    "recipients": ["renatobolf@bemol.com.br"],
    "bcc": ["caiomiguel@bemol.com.br"]
}

# Configurações de logging
LOGGING_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": "/dbfs/FileStore/logs/scraping.log"
}
