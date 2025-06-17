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
    Limpa e padroniza um preço.
    
    Args:
        preco: Preço a ser limpo (string ou float)
        
    Returns:
        float: Preço limpo ou None se inválido
    """
    if preco is None:
        return None
        
    try:
        # Se já for float, retorna como está
        if isinstance(preco, float):
            return preco
            
        # Converte para string e remove espaços
        preco_str = str(preco).strip()
        
        # Remove "ou R$" e qualquer espaço em branco
        preco_str = preco_str.replace('ou R$', '').strip()
        
        # Remove pontos de milhar
        preco_str = preco_str.replace('.', '')
        
        # Substitui vírgula por ponto
        preco_str = preco_str.replace(',', '.')
        
        # Converte para float
        return float(preco_str)
        
    except (ValueError, AttributeError) as e:
        logger.warning(f"Não foi possível converter o preço '{preco}' para float. Erro: {str(e)}")
        return None

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