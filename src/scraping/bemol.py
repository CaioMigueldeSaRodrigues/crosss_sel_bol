from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import logging

def scrape_bemol(spark):
    """
    Obtém produtos da Bemol a partir da tabela Delta
    
    Args:
        spark (SparkSession): Sessão Spark
        
    Returns:
        DataFrame: DataFrame Spark com os produtos
    """
    try:
        # Busca dados da tabela Delta
        df = spark.sql("""
            SELECT 
                title,
                CAST(price AS DOUBLE) as price,
                link as url,
                'bemol' as source,
                CURRENT_TIMESTAMP() as extraction_date
            FROM bol.feed_varejo_vtex
            WHERE availability = 'disponível'
        """)
        
        logging.info(f"Total de produtos obtidos da Bemol: {df.count()}")
        return df
        
    except Exception as e:
        logging.error(f"Erro ao obter produtos da Bemol: {str(e)}")
        return None 