from pyspark.sql.functions import col, regexp_replace, when, lit
from pyspark.sql.types import DoubleType
import logging
from src.config import LOGGING_CONFIG

# Configuração de logging
logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG["level"]),
    format=LOGGING_CONFIG["format"]
)
logger = logging.getLogger(__name__)

def clean_dataframe_prices(df, column_name="price"):
    """
    Limpa e padroniza os preços no DataFrame Spark usando funções nativas.
    """
    if df is None:
        raise ValueError("DataFrame não pode ser None")
    if column_name not in df.columns:
        raise ValueError(f"Coluna '{column_name}' não encontrada no DataFrame")
    try:
        # Remove 'ou R$', espaços, pontos de milhar, substitui vírgula por ponto e converte para float
        df = df.withColumn(
            column_name,
            regexp_replace(col(column_name), r"ou R\$|\s", "")
        )
        df = df.withColumn(
            column_name,
            regexp_replace(col(column_name), r"\.", "")
        )
        df = df.withColumn(
            column_name,
            regexp_replace(col(column_name), ",", ".")
        )
        df = df.withColumn(
            column_name,
            col(column_name).cast("double")
        )
        df = df.withColumn(
            column_name,
            when(col(column_name).isNull() | (col(column_name) <= 0), None).otherwise(col(column_name))
        )
        logger.info(f"Limpeza de preços concluída com sucesso para {df.count()} registros")
        return df
    except Exception as e:
        logger.error(f"Erro durante a limpeza de preços: {str(e)}")
        raise 