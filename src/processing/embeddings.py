from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import ArrayType, FloatType
import numpy as np
from sentence_transformers import SentenceTransformer
import logging
import pandas as pd
from ..config import EMBEDDING_MODEL, BATCH_SIZE

def generate_embeddings_from_delta(spark, table_name="bol.feed_varejo_vtex", batch_size=32):
    """
    Gera embeddings para os títulos dos produtos a partir de uma tabela Delta
    
    Args:
        spark (SparkSession): Sessão Spark
        table_name (str): Nome completo da tabela Delta (catalog.schema.table)
        batch_size (int): Tamanho do batch para processamento
        
    Returns:
        DataFrame: DataFrame com embeddings adicionados
    """
    try:
        # Carrega o modelo
        model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Define UDF para gerar embeddings
        @udf(returnType=ArrayType(FloatType()))
        def generate_embedding(text):
            if text is None:
                return None
            embedding = model.encode(text)
            return embedding.tolist()
        
        # Busca dados da tabela Delta
        df = spark.sql(f"""
            SELECT title, price, link
            FROM {table_name}
            WHERE availability = 'disponível'
        """)
        
        # Gera embeddings
        df = df.withColumn("embedding", generate_embedding(col("title")))
        
        logging.info(f"Geração de embeddings concluída com sucesso para {df.count()} produtos")
        return df
        
    except Exception as e:
        logging.error(f"Erro durante a geração de embeddings: {str(e)}")
        return None

def generate_dataframe_embeddings(df, column_name="title", batch_size=32):
    """
    Gera embeddings para os títulos dos produtos usando Sentence Transformers
    
    Args:
        df (DataFrame): DataFrame Spark com os dados
        column_name (str): Nome da coluna de texto para gerar embeddings
        batch_size (int): Tamanho do batch para processamento
        
    Returns:
        DataFrame: DataFrame com embeddings adicionados
    """
    try:
        # Carrega o modelo
        model = SentenceTransformer('all-MiniLM-L6-v2')
        
        # Define UDF para gerar embeddings
        @udf(returnType=ArrayType(FloatType()))
        def generate_embedding(text):
            if text is None:
                return None
            embedding = model.encode(text)
            return embedding.tolist()
        
        # Gera embeddings
        df = df.withColumn("embedding", generate_embedding(col(column_name)))
        
        logging.info("Geração de embeddings concluída com sucesso")
        return df
        
    except Exception as e:
        logging.error(f"Erro durante a geração de embeddings: {str(e)}")
        return df

def generate_text_embeddings(texts, model_name=EMBEDDING_MODEL, batch_size=BATCH_SIZE):
    """
    Gera embeddings para uma lista de textos usando o modelo especificado.
    
    Args:
        texts (list): Lista de textos para gerar embeddings
        model_name (str): Nome do modelo a ser usado
        batch_size (int): Tamanho do batch para processamento
        
    Returns:
        list: Lista de embeddings gerados
    """
    try:
        # Carrega o modelo
        model = SentenceTransformer(model_name)
        
        # Gera embeddings em batches
        embeddings = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch_embeddings = model.encode(batch, show_progress_bar=True)
            embeddings.extend(batch_embeddings)
            
        return embeddings
        
    except Exception as e:
        print(f"Erro ao gerar embeddings: {str(e)}")
        raise
