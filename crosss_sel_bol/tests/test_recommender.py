"""
Testes do recomendador de produtos
"""

import pytest
import pandas as pd
from src.models.recommender import ProductRecommender

def test_recommender_initialization(recommendation_config):
    """
    Testa a inicialização do recomendador
    """
    recommender = ProductRecommender(recommendation_config)
    
    assert recommender.min_support == recommendation_config['min_support']
    assert recommender.min_confidence == recommendation_config['min_confidence']
    assert recommender.max_recommendations == recommendation_config['max_recommendations']
    assert recommender.similarity_threshold == recommendation_config['similarity_threshold']
    assert recommender.product_data is None
    assert recommender.similarity_matrix is None
    assert recommender.association_rules is None

def test_load_data(sample_product_data, recommendation_config):
    """
    Testa o carregamento de dados
    """
    recommender = ProductRecommender(recommendation_config)
    recommender.load_data(sample_product_data)
    
    assert isinstance(recommender.product_data, pd.DataFrame)
    assert len(recommender.product_data) == len(sample_product_data)
    assert all(col in recommender.product_data.columns for col in sample_product_data.columns)

def test_build_similarity_matrix(sample_product_data, recommendation_config):
    """
    Testa a construção da matriz de similaridade
    """
    recommender = ProductRecommender(recommendation_config)
    recommender.load_data(sample_product_data)
    recommender.build_similarity_matrix()
    
    assert recommender.similarity_matrix is not None
    assert recommender.similarity_matrix.shape == (len(sample_product_data), len(sample_product_data))

def test_generate_association_rules(sample_transaction_data, recommendation_config):
    """
    Testa a geração de regras de associação
    """
    recommender = ProductRecommender(recommendation_config)
    transactions = sample_transaction_data.groupby('pedido_id')['produto_id'].apply(list).tolist()
    recommender.generate_association_rules(transactions)
    
    assert recommender.association_rules is not None
    assert len(recommender.association_rules) > 0

def test_get_similar_products(sample_product_data, recommendation_config):
    """
    Testa a obtenção de produtos similares
    """
    recommender = ProductRecommender(recommendation_config)
    recommender.load_data(sample_product_data)
    recommender.build_similarity_matrix()
    
    similar_products = recommender.get_similar_products('1')
    assert isinstance(similar_products, list)
    assert len(similar_products) <= recommendation_config['max_recommendations']

def test_get_cross_sell_recommendations(sample_transaction_data, recommendation_config):
    """
    Testa a obtenção de recomendações de cross-selling
    """
    recommender = ProductRecommender(recommendation_config)
    transactions = sample_transaction_data.groupby('pedido_id')['produto_id'].apply(list).tolist()
    recommender.generate_association_rules(transactions)
    
    cross_sell = recommender.get_cross_sell_recommendations('1')
    assert isinstance(cross_sell, list)
    assert len(cross_sell) <= recommendation_config['max_recommendations']

def test_get_promotional_recommendations(sample_product_data, recommendation_config):
    """
    Testa a obtenção de recomendações promocionais
    """
    recommender = ProductRecommender(recommendation_config)
    recommender.load_data(sample_product_data)
    
    promocoes = recommender.get_promotional_recommendations()
    assert isinstance(promocoes, list)
    assert len(promocoes) <= recommendation_config['max_recommendations']

def test_get_recommendations(sample_product_data, sample_transaction_data, recommendation_config):
    """
    Testa a obtenção de todas as recomendações
    """
    recommender = ProductRecommender(recommendation_config)
    recommender.load_data(sample_product_data)
    recommender.build_similarity_matrix()
    
    transactions = sample_transaction_data.groupby('pedido_id')['produto_id'].apply(list).tolist()
    recommender.generate_association_rules(transactions)
    
    recomendacoes = recommender.get_recommendations('1')
    assert isinstance(recomendacoes, dict)
    assert 'similares' in recomendacoes
    assert 'cross_selling' in recomendacoes
    assert 'promocionais' in recomendacoes
    
    for tipo, recs in recomendacoes.items():
        assert isinstance(recs, list)
        assert len(recs) <= recommendation_config['max_recommendations'] 