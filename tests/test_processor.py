"""
Testes do processador de dados
"""

import pytest
import pandas as pd
from src.data.processor import DataProcessor

def test_process_product_data(sample_product_data):
    """
    Testa o processamento de dados de produtos
    """
    processor = DataProcessor()
    processed_data = processor.process_product_data(sample_product_data)
    
    assert isinstance(processed_data, pd.DataFrame)
    assert len(processed_data) == len(sample_product_data)
    assert all(col in processed_data.columns for col in processor.required_columns)
    assert processed_data['preco'].dtype == float
    assert processed_data['promocao'].dtype == bool

def test_process_transaction_data(sample_transaction_data):
    """
    Testa o processamento de dados de transações
    """
    processor = DataProcessor()
    transactions = processor.process_transaction_data(sample_transaction_data)
    
    assert isinstance(transactions, list)
    assert len(transactions) == 3  # Número de pedidos únicos
    assert all(isinstance(t, list) for t in transactions)
    assert all(isinstance(p, str) for t in transactions for p in t)

def test_process_promotional_data(sample_promotional_data):
    """
    Testa o processamento de dados promocionais
    """
    processor = DataProcessor()
    processed_data = processor.process_promotional_data(sample_promotional_data)
    
    assert isinstance(processed_data, pd.DataFrame)
    assert len(processed_data) == len(sample_promotional_data)
    assert 'desconto' in processed_data.columns
    assert processed_data['desconto'].dtype == float
    assert processed_data['desconto'].max() <= 100
    assert processed_data['desconto'].min() >= 0

def test_merge_data(sample_product_data, sample_promotional_data):
    """
    Testa a mesclagem de dados de produtos e promoções
    """
    processor = DataProcessor()
    merged_data = processor.merge_data(sample_product_data, sample_promotional_data)
    
    assert isinstance(merged_data, pd.DataFrame)
    assert len(merged_data) == len(sample_product_data)
    assert 'preco_promocional' in merged_data.columns
    assert 'desconto' in merged_data.columns
    assert merged_data['promocao'].sum() == 2  # Número de produtos em promoção

def test_prepare_training_data(sample_product_data, sample_transaction_data):
    """
    Testa a preparação de dados para treinamento
    """
    processor = DataProcessor()
    products, transactions = processor.prepare_training_data(
        sample_product_data,
        sample_transaction_data
    )
    
    assert isinstance(products, pd.DataFrame)
    assert isinstance(transactions, list)
    assert len(products) == len(sample_product_data)
    assert len(transactions) == 3  # Número de pedidos únicos 