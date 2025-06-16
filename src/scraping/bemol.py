from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import logging
from src.config import LOGGING_CONFIG, STORAGE_CONFIG

# Configuração de logging
logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG["level"]),
    format=LOGGING_CONFIG["format"]
)
logger = logging.getLogger(__name__)

def scrape_bemol(spark: SparkSession):
    """
    Obtém produtos da Bemol a partir da tabela Delta
    
    Args:
        spark (SparkSession): Sessão Spark
        
    Returns:
        DataFrame: DataFrame Spark com os produtos
    """
    try:
        # Busca dados da tabela Delta
        df = spark.sql(f"""
            SELECT 
                title,
                CAST(price AS DOUBLE) as price,
                link as url,
                'bemol' as source,
                current_timestamp() as extraction_date
            FROM {STORAGE_CONFIG['delta_tables']['bemol']}
            WHERE availability = 'disponível'
        """)
        
        logger.info(f"Total de produtos obtidos da Bemol: {df.count()}")
        return df
        
    except Exception as e:
        logger.error(f"Erro ao obter produtos da Bemol: {str(e)}")
        return None 