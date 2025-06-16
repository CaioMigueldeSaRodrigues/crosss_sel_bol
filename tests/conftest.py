"""
Configuração do pytest
"""

import pytest
import pandas as pd
import numpy as np
from typing import Dict, Any

@pytest.fixture
def sample_product_data() -> pd.DataFrame:
    """
    Fixture com dados de exemplo de produtos
    """
    return pd.DataFrame({
        'id': ['1', '2', '3'],
        'nome': ['Produto 1', 'Produto 2', 'Produto 3'],
        'categoria': ['Categoria 1', 'Categoria 1', 'Categoria 2'],
        'preco': [100.0, 200.0, 300.0],
        'promocao': [True, False, True],
        'data_atualizacao': ['2023-01-01', '2023-01-01', '2023-01-01']
    })

@pytest.fixture
def sample_transaction_data() -> pd.DataFrame:
    """
    Fixture com dados de exemplo de transações
    """
    return pd.DataFrame({
        'pedido_id': ['1', '1', '2', '2', '3'],
        'produto_id': ['1', '2', '2', '3', '1'],
        'quantidade': [1, 2, 1, 1, 1],
        'valor_total': [100.0, 400.0, 200.0, 300.0, 100.0],
        'data_pedido': ['2023-01-01', '2023-01-01', '2023-01-02', '2023-01-02', '2023-01-03']
    })

@pytest.fixture
def sample_promotional_data() -> pd.DataFrame:
    """
    Fixture com dados de exemplo de promoções
    """
    return pd.DataFrame({
        'id': ['1', '3'],
        'nome': ['Produto 1', 'Produto 3'],
        'preco_original': [100.0, 300.0],
        'preco_promocional': [80.0, 250.0]
    })

@pytest.fixture
def recommendation_config() -> Dict[str, Any]:
    """
    Fixture com configurações de recomendação
    """
    return {
        'min_support': 0.01,
        'min_confidence': 0.5,
        'max_recommendations': 5,
        'similarity_threshold': 0.7
    } 