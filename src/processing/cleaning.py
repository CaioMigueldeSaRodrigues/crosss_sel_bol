from pyspark.sql.functions import col, regexp_replace, when, lit, udf
from pyspark.sql.types import DoubleType
import logging
import re
from src.config import LOGGING_CONFIG

# Configuração de logging
logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG["level"]),
    format=LOGGING_CONFIG["format"]
)
logger = logging.getLogger(__name__)

def limpar_preco(preco):
    """
    Limpa e converte um preço para float.
    
    Args:
        preco: Valor do preço a ser limpo (string, float ou outro tipo)
        
    Returns:
        float: Preço limpo e convertido, ou 0.0 se não for possível converter
    """
    if preco is None:
        return 0.0
        
    try:
        preco_str = str(preco)
        # Remove caracteres especiais e espaços
        preco_str = preco_str.replace('\xa0', ' ').replace('R$', '').replace('ou', '').strip()
        
        # Tenta encontrar um padrão de preço brasileiro (ex: 1.234,56)
        match = re.search(r'(\d{1,3}(?:\.\d{3})*,\d{2})', preco_str)
        if match:
            preco_str = match.group(1).replace('.', '').replace(',', '.')
            return float(preco_str)
            
        # Se não encontrar o padrão, tenta converter diretamente
        preco_str = preco_str.replace('.', '').replace(',', '.')
        return float(preco_str)
    except (ValueError, TypeError, AttributeError) as e:
        logger.warning(f"Erro ao converter preço '{preco}': {str(e)}")
        return 0.0

# Registra a função como UDF do Spark
limpar_preco_udf = udf(limpar_preco, DoubleType())

def clean_dataframe_prices(df, column_name="price"):
    """
    Limpa e padroniza os preços no DataFrame Spark.
    
    Args:
        df (DataFrame): DataFrame Spark com os dados
        column_name (str): Nome da coluna de preços
        
    Returns:
        DataFrame: DataFrame com preços limpos
        
    Raises:
        ValueError: Se o DataFrame for None ou a coluna não existir
    """
    if df is None:
        raise ValueError("DataFrame não pode ser None")
        
    if column_name not in df.columns:
        raise ValueError(f"Coluna '{column_name}' não encontrada no DataFrame")
        
    try:
        # Aplica a função de limpeza de preços
        df = df.withColumn(
            column_name,
            limpar_preco_udf(col(column_name))
        )
        
        # Remove valores nulos ou negativos
        df = df.withColumn(
            column_name,
            when(col(column_name).isNull() | (col(column_name) <= 0), None).otherwise(col(column_name))
        )
        
        logger.info(f"Limpeza de preços concluída com sucesso para {df.count()} registros")
        return df
        
    except Exception as e:
        logger.error(f"Erro durante a limpeza de preços: {str(e)}")
        raise 