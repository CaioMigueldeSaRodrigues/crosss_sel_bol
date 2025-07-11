from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import ArrayType, FloatType, StructType, StructField, StringType
from sentence_transformers import SentenceTransformer
import logging
import pandas as pd
from src.config import PROCESSING_CONFIG, LOGGING_CONFIG

# Configuração de logging
logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG["level"]),
    format=LOGGING_CONFIG["format"]
)
logger = logging.getLogger(__name__)

def generate_dataframe_embeddings(df, column_name="title"):
    """
    Gera embeddings para os títulos dos produtos usando Sentence Transformers.
    Converte o Spark DataFrame para Pandas, processa, e converte de volta para Spark.
    
    Args:
        df (DataFrame): DataFrame Spark com os dados.
        column_name (str): Nome da coluna de texto para gerar embeddings.
        
    Returns:
        DataFrame: DataFrame Spark com embeddings adicionados.
    """
    if df is None or df.isEmpty():
        logger.warning("DataFrame vazio recebido para geração de embeddings")
        return df

    try:
        # Carrega o modelo de embeddings
        model = SentenceTransformer(PROCESSING_CONFIG["embedding_model"])
        logger.info(f"Modelo {PROCESSING_CONFIG['embedding_model']} carregado com sucesso")

        # Converte o DataFrame Spark para Pandas DataFrame
        df_pandas = df.toPandas()
        logger.info(f"DataFrame convertido para Pandas com {len(df_pandas)} linhas")

        # Gera embeddings na coluna especificada no DataFrame Pandas
        # Garante que None ou NaN em 'title' não causem erro de encode
        df_pandas["embedding"] = df_pandas[column_name].apply(
            lambda x: model.encode(x).tolist() if pd.notna(x) else None
        )
        logger.info("Embeddings gerados com sucesso")

        # Preserva o schema original e adiciona o campo 'embedding'
        existing_fields = df.schema.fields
        # Remove o campo 'embedding' se já existir (para idempotência)
        existing_fields = [f for f in existing_fields if f.name != "embedding"]
        output_schema = StructType(existing_fields + [StructField("embedding", ArrayType(FloatType()), True)])

        # Converte o DataFrame Pandas de volta para Spark DataFrame
        spark_df_with_embeddings = df.sparkSession.createDataFrame(df_pandas, schema=output_schema)
        logger.info(f"DataFrame convertido de volta para Spark com {spark_df_with_embeddings.count()} linhas")

        return spark_df_with_embeddings

    except Exception as e:
        logger.error(f"Erro durante a geração de embeddings: {str(e)}")
        # Em caso de erro, retorna um DataFrame Spark vazio com o schema esperado
        empty_schema = df.schema
        if "embedding" not in [f.name for f in empty_schema.fields]:
            empty_schema = StructType(empty_schema.fields + [StructField("embedding", ArrayType(FloatType()), True)])
        return df.sparkSession.createDataFrame([], schema=empty_schema)

def generate_text_embeddings(texts, model_name=PROCESSING_CONFIG["embedding_model"], batch_size=PROCESSING_CONFIG["batch_size"]):
    """
    Gera embeddings para uma lista de textos usando o modelo especificado.
    
    Args:
        texts (list): Lista de textos para gerar embeddings
        model_name (str): Nome do modelo a ser usado
        batch_size (int): Tamanho do batch para processamento
        
    Returns:
        list: Lista de embeddings gerados
        
    Raises:
        ValueError: Se a lista de textos estiver vazia
        Exception: Para outros erros durante o processamento
    """
    if not texts:
        raise ValueError("Lista de textos vazia recebida")

    try:
        # Carrega o modelo
        model = SentenceTransformer(model_name)
        logger.info(f"Modelo {model_name} carregado com sucesso")
        
        # Gera embeddings em batches
        embeddings = []
        total_batches = (len(texts) + batch_size - 1) // batch_size
        
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            current_batch = (i // batch_size) + 1
            logger.info(f"Processando batch {current_batch}/{total_batches}")
            
            batch_embeddings = model.encode(batch, show_progress_bar=True)
            embeddings.extend(batch_embeddings)
            
        logger.info(f"Geração de embeddings concluída para {len(texts)} textos")
        return embeddings
        
    except Exception as e:
        logger.error(f"Erro ao gerar embeddings: {str(e)}")
        raise
