from pyspark.sql.functions import col, udf, lit, current_timestamp
from pyspark.sql.types import DoubleType, ArrayType, FloatType
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer
import logging
import pandas as pd
from ..config import SIMILARITY_THRESHOLD, EMBEDDING_MODEL

# Inicializa o modelo de embeddings
model = SentenceTransformer(EMBEDDING_MODEL)

@udf(returnType=ArrayType(FloatType()))
def generate_embedding(text):
    """
    Gera embedding para um texto usando o modelo SentenceTransformer
    """
    if text is None:
        return None
    try:
        embedding = model.encode(text)
        return embedding.tolist()
    except Exception as e:
        logging.error(f"Erro ao gerar embedding: {str(e)}")
        return None

@udf(returnType=DoubleType())
def calculate_similarity(embedding1, embedding2):
    """
    Calcula a similaridade de cosseno entre dois embeddings
    
    Args:
        embedding1 (list): Primeiro embedding
        embedding2 (list): Segundo embedding
        
    Returns:
        float: Score de similaridade
    """
    if embedding1 is None or embedding2 is None:
        return 0.0
    
    try:
        # Converte para arrays numpy
        emb1 = np.array(embedding1).reshape(1, -1)
        emb2 = np.array(embedding2).reshape(1, -1)
        
        # Calcula similaridade
        similarity = cosine_similarity(emb1, emb2)[0][0]
        return float(similarity)
    except Exception as e:
        logging.error(f"Erro ao calcular similaridade: {str(e)}")
        return 0.0

def match_products(df_magalu, df_bemol, similarity_threshold=SIMILARITY_THRESHOLD):
    """
    Encontra produtos correspondentes entre Magazine Luiza e Bemol usando similaridade de cosseno.
    
    Args:
        df_magalu (DataFrame): DataFrame Spark com produtos do Magazine Luiza
        df_bemol (DataFrame): DataFrame Spark com produtos da Bemol
        similarity_threshold (float): Limiar de similaridade para considerar produtos como correspondentes
        
    Returns:
        DataFrame: DataFrame Spark com produtos correspondentes e seus scores de similaridade
    """
    try:
        # Adiciona embeddings e timestamps
        df_magalu = df_magalu.withColumn("embedding", generate_embedding(col("title"))) \
                            .withColumn("extraction_date", current_timestamp())
        
        df_bemol = df_bemol.withColumn("embedding", generate_embedding(col("title"))) \
                          .withColumn("extraction_date", current_timestamp())

        # Renomeia colunas para evitar conflitos
        df_magalu_renamed = df_magalu.withColumnRenamed("title", "magalu_title") \
                                    .withColumnRenamed("price", "magalu_price") \
                                    .withColumnRenamed("url", "magalu_url") \
                                    .withColumnRenamed("categoria", "magalu_categoria")

        df_bemol_renamed = df_bemol.withColumnRenamed("title", "bemol_title") \
                                  .withColumnRenamed("price", "bemol_price") \
                                  .withColumnRenamed("url", "bemol_url") \
                                  .withColumnRenamed("categoria", "bemol_categoria")

        # Realiza cross join e calcula similaridade
        joined_df = df_magalu_renamed.crossJoin(df_bemol_renamed)
        joined_df = joined_df.withColumn(
            "similarity_score",
            calculate_similarity(col("embedding"), col("embedding"))
        )

        # Filtra e seleciona colunas
        df_matches = joined_df.filter(col("similarity_score") >= similarity_threshold) \
                            .select(
                                col("magalu_title"),
                                col("magalu_price"),
                                col("magalu_url"),
                                col("magalu_categoria"),
                                col("magalu_extraction_date"),
                                col("bemol_title"),
                                col("bemol_price"),
                                col("bemol_url"),
                                col("bemol_categoria"),
                                col("bemol_extraction_date"),
                                col("similarity_score")
                            ).orderBy(col("similarity_score").desc())

        logging.info(f"Matching de produtos concluído. {df_matches.count()} pares encontrados.")
        return df_matches

    except Exception as e:
        logging.error(f"Erro durante o matching de produtos: {str(e)}")
        return None 