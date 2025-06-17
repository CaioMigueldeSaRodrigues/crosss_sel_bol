from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import DoubleType
import logging
from src.config import LOGGING_CONFIG

# Configuração de logging
logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG["level"]),
    format=LOGGING_CONFIG["format"]
)
logger = logging.getLogger(__name__)

def calculate_price_difference(df, price1_col="price1", price2_col="price2", diff_col="price_diff"):
    """
    Calcula a diferença percentual entre dois preços.
    
    Args:
        df (DataFrame): DataFrame Spark com os preços
        price1_col (str): Nome da coluna do primeiro preço
        price2_col (str): Nome da coluna do segundo preço
        diff_col (str): Nome da coluna para armazenar a diferença
        
    Returns:
        DataFrame: DataFrame com a coluna de diferença adicionada
    """
    if df is None:
        raise ValueError("DataFrame não pode ser None")
        
    if not all(col in df.columns for col in [price1_col, price2_col]):
        raise ValueError(f"Colunas de preço não encontradas no DataFrame")
        
    try:
        # Calcula a diferença percentual
        df = df.withColumn(
            diff_col,
            when(
                (col(price1_col).isNotNull() & col(price2_col).isNotNull()) &
                (col(price1_col) > 0 & col(price2_col) > 0),
                ((col(price2_col) - col(price1_col)) / col(price1_col)) * 100
            ).otherwise(None)
        )
        
        logger.info(f"Diferença de preços calculada para {df.count()} registros")
        return df
        
    except Exception as e:
        logger.error(f"Erro ao calcular diferença de preços: {str(e)}")
        raise

def filter_price_range(df, price_col="price", min_price=None, max_price=None):
    """
    Filtra registros por faixa de preço.
    
    Args:
        df (DataFrame): DataFrame Spark com os preços
        price_col (str): Nome da coluna de preço
        min_price (float): Preço mínimo (opcional)
        max_price (float): Preço máximo (opcional)
        
    Returns:
        DataFrame: DataFrame filtrado
    """
    if df is None:
        raise ValueError("DataFrame não pode ser None")
        
    if price_col not in df.columns:
        raise ValueError(f"Coluna '{price_col}' não encontrada no DataFrame")
        
    try:
        # Aplica filtros de preço
        if min_price is not None:
            df = df.filter(col(price_col) >= min_price)
            
        if max_price is not None:
            df = df.filter(col(price_col) <= max_price)
            
        logger.info(f"Filtro de preço aplicado: {df.count()} registros restantes")
        return df
        
    except Exception as e:
        logger.error(f"Erro ao filtrar por faixa de preço: {str(e)}")
        raise

def normalize_prices(df, price_col="price", target_col="normalized_price"):
    """
    Normaliza preços para uma escala de 0 a 1.
    
    Args:
        df (DataFrame): DataFrame Spark com os preços
        price_col (str): Nome da coluna de preço
        target_col (str): Nome da coluna para o preço normalizado
        
    Returns:
        DataFrame: DataFrame com preços normalizados
    """
    if df is None:
        raise ValueError("DataFrame não pode ser None")
        
    if price_col not in df.columns:
        raise ValueError(f"Coluna '{price_col}' não encontrada no DataFrame")
        
    try:
        # Calcula min e max
        min_price = df.select(price_col).filter(col(price_col).isNotNull()).agg({price_col: "min"}).collect()[0][0]
        max_price = df.select(price_col).filter(col(price_col).isNotNull()).agg({price_col: "max"}).collect()[0][0]
        
        if min_price == max_price:
            logger.warning("Todos os preços são iguais, normalização não terá efeito")
            return df.withColumn(target_col, lit(1.0))
            
        # Normaliza os preços
        df = df.withColumn(
            target_col,
            (col(price_col) - min_price) / (max_price - min_price)
        )
        
        logger.info(f"Preços normalizados para {df.count()} registros")
        return df
        
    except Exception as e:
        logger.error(f"Erro ao normalizar preços: {str(e)}")
        raise
