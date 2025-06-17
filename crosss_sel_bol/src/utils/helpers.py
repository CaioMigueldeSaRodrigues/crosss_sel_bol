"""
Funções utilitárias para o sistema de recomendação
"""

import logging
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime

def setup_logging(name: str) -> logging.Logger:
    """
    Configura o logging para o módulo
    
    Args:
        name: Nome do módulo
        
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """
    Valida se o DataFrame possui todas as colunas necessárias
    
    Args:
        df: DataFrame a ser validado
        required_columns: Lista de colunas obrigatórias
        
    Returns:
        True se todas as colunas existem, False caso contrário
    """
    return all(col in df.columns for col in required_columns)

def format_product_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Formata os dados do produto para o padrão do sistema
    
    Args:
        data: Dicionário com dados do produto
        
    Returns:
        Dicionário formatado
    """
    return {
        'id': str(data.get('id', '')),
        'nome': str(data.get('nome', '')).strip(),
        'categoria': str(data.get('categoria', '')).strip(),
        'preco': float(data.get('preco', 0.0)),
        'promocao': bool(data.get('promocao', False)),
        'data_atualizacao': datetime.now().isoformat()
    }

def calculate_similarity_score(item1: Dict[str, Any], item2: Dict[str, Any]) -> float:
    """
    Calcula o score de similaridade entre dois produtos
    
    Args:
        item1: Dicionário com dados do primeiro produto
        item2: Dicionário com dados do segundo produto
        
    Returns:
        Score de similaridade entre 0 e 1
    """
    # Implementar lógica de cálculo de similaridade
    # Por exemplo, usando similaridade de cosseno ou outras métricas
    return 0.0

def filter_recommendations(
    recommendations: List[Dict[str, Any]],
    max_items: int = 5,
    min_score: float = 0.5
) -> List[Dict[str, Any]]:
    """
    Filtra e ordena as recomendações
    
    Args:
        recommendations: Lista de recomendações
        max_items: Número máximo de itens
        min_score: Score mínimo para inclusão
        
    Returns:
        Lista filtrada e ordenada de recomendações
    """
    filtered = [
        rec for rec in recommendations
        if rec.get('score', 0) >= min_score
    ]
    
    sorted_recs = sorted(
        filtered,
        key=lambda x: x.get('score', 0),
        reverse=True
    )
    
    return sorted_recs[:max_items] 