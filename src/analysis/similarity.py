from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import DoubleType
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import logging

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

def match_products(df_magalu, df_bemol, similarity_threshold=0.7):
    """
    Realiza o matching de produtos entre Magazine Luiza e Bemol
    
    Args:
        df_magalu (DataFrame): DataFrame com produtos do Magazine Luiza
        df_bemol (DataFrame): DataFrame com produtos da Bemol
        similarity_threshold (float): Limiar de similaridade para matching
        
    Returns:
        DataFrame: DataFrame com produtos pareados
    """
    try:
        # Registra UDF para cálculo de similaridade
        similarity_udf = udf(calculate_similarity, DoubleType())
        
        # Adiciona prefixos nas colunas para evitar conflitos
        df_magalu = df_magalu.select([col(c).alias(f"magalu_{c}") for c in df_magalu.columns])
        df_bemol = df_bemol.select([col(c).alias(f"bemol_{c}") for c in df_bemol.columns])
        
        # Realiza cross join
        df_cross = df_magalu.crossJoin(df_bemol)
        
        # Calcula similaridade
        df_cross = df_cross.withColumn(
            "similarity",
            similarity_udf(col("magalu_embedding"), col("bemol_embedding"))
        )
        
        # Filtra por threshold
        df_matches = df_cross.filter(col("similarity") >= similarity_threshold)
        
        # Ordena por similaridade
        df_matches = df_matches.orderBy(col("similarity").desc())
        
        logging.info(f"Matching concluído. {df_matches.count()} matches encontrados")
        return df_matches
        
    except Exception as e:
        logging.error(f"Erro durante o matching de produtos: {str(e)}")
        return None 