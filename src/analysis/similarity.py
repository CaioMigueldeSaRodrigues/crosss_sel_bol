from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import DoubleType, ArrayType, FloatType
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import logging
import pandas as pd
from ..config import SIMILARITY_THRESHOLD

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
    Encontra produtos correspondentes entre Magazine Luiza e Bemol usando similaridade de cosseno em DataFrames Spark.
    
    Args:
        df_magalu (DataFrame): DataFrame Spark com produtos do Magazine Luiza, incluindo 'magalu_embedding'.
        df_bemol (DataFrame): DataFrame Spark com produtos da Bemol, incluindo 'bemol_embedding'.
        similarity_threshold (float): Limiar de similaridade para considerar produtos como correspondentes.
        
    Returns:
        DataFrame: DataFrame Spark com produtos correspondentes e seus scores de similaridade.
    """
    try:
        # Renomear colunas para evitar conflitos após o join
        df_magalu_renamed = df_magalu.withColumnRenamed("title", "magalu_title") \
                                      .withColumnRenamed("price", "magalu_price") \
                                      .withColumnRenamed("url", "magalu_url") \
                                      .withColumnRenamed("source", "magalu_source") \
                                      .withColumnRenamed("extraction_date", "magalu_extraction_date")

        df_bemol_renamed = df_bemol.withColumnRenamed("title", "bemol_title") \
                                    .withColumnRenamed("price", "bemol_price") \
                                    .withColumnRenamed("url", "bemol_url") \
                                    .withColumnRenamed("source", "bemol_source") \
                                    .withColumnRenamed("extraction_date", "bemol_extraction_date")

        # Realizar um cross join para comparar todos os pares
        joined_df = df_magalu_renamed.crossJoin(df_bemol_renamed)

        # Calcular a similaridade usando a UDF
        joined_df = joined_df.withColumn(
            "similarity_score",
            calculate_similarity(col("magalu_embedding"), col("bemol_embedding"))
        )

        # Filtrar por similaridade e selecionar as colunas desejadas
        df_matches = joined_df.filter(col("similarity_score") >= similarity_threshold) \
                                .select(
                                    col("magalu_title"),
                                    col("magalu_price"),
                                    col("magalu_url"),
                                    col("magalu_source"),
                                    col("magalu_extraction_date"),
                                    col("bemol_title"),
                                    col("bemol_price"),
                                    col("bemol_url"),
                                    col("bemol_source"),
                                    col("bemol_extraction_date"),
                                    col("similarity_score")
                                ).orderBy(col("similarity_score").desc())

        logging.info(f"Matching de produtos concluído. {df_matches.count()} pares encontrados.")
        return df_matches

    except Exception as e:
        logging.error(f"Erro durante o matching de produtos: {str(e)}")
        return None 