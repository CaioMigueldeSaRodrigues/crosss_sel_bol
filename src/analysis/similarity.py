from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import DoubleType
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import logging
import pandas as pd
from ..config import SIMILARITY_THRESHOLD

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

def match_products(bemol_embeddings, magalu_embeddings, bemol_products, magalu_products, threshold=SIMILARITY_THRESHOLD):
    """
    Encontra produtos correspondentes entre Bemol e Magazine Luiza usando similaridade de cosseno.
    
    Args:
        bemol_embeddings (list): Embeddings dos produtos da Bemol
        magalu_embeddings (list): Embeddings dos produtos do Magazine Luiza
        bemol_products (pd.DataFrame): DataFrame com produtos da Bemol
        magalu_products (pd.DataFrame): DataFrame com produtos do Magazine Luiza
        threshold (float): Limiar de similaridade para considerar produtos como correspondentes
        
    Returns:
        pd.DataFrame: DataFrame com produtos correspondentes
    """
    try:
        # Calcula similaridade de cosseno
        similarity_matrix = cosine_similarity(bemol_embeddings, magalu_embeddings)
        
        # Encontra pares de produtos com similaridade acima do threshold
        matches = []
        for i in range(len(bemol_products)):
            for j in range(len(magalu_products)):
                similarity = similarity_matrix[i][j]
                if similarity >= threshold:
                    matches.append({
                        'bemol_id': bemol_products.iloc[i]['id'],
                        'bemol_title': bemol_products.iloc[i]['title'],
                        'bemol_price': bemol_products.iloc[i]['price'],
                        'magalu_id': magalu_products.iloc[j]['id'],
                        'magalu_title': magalu_products.iloc[j]['title'],
                        'magalu_price': magalu_products.iloc[j]['price'],
                        'similarity_score': similarity
                    })
        
        # Cria DataFrame com matches
        matches_df = pd.DataFrame(matches)
        
        # Ordena por score de similaridade
        matches_df = matches_df.sort_values('similarity_score', ascending=False)
        
        return matches_df
        
    except Exception as e:
        print(f"Erro ao fazer matching de produtos: {str(e)}")
        raise 