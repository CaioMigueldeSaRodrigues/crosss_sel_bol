import pytest
import pandas as pd
from src.data_processing import preprocessar_dados_cliente
from src.model_training import train_classification_model

def test_preprocessar_dados_cliente():
    # Dados fictícios mínimos
    df_customers = pd.DataFrame({
        'customer_unique_id': ['c1'],
        'customer_city': ['city1'],
        'customer_state': ['state1']
    })
    df_orders = pd.DataFrame({
        'order_id': ['o1'],
        'customer_unique_id': ['c1']
    })
    df_final = preprocessar_dados_cliente(df_customers, df_orders)
    assert not df_final.empty

def test_train_classification_model(monkeypatch):
    # Dados fictícios mínimos
    df = pd.DataFrame({
        'customer_unique_id': ['c1', 'c2'],
        'order_id': ['o1', 'o2'],
        'customer_city': ['city1', 'city2'],
        'customer_state': ['state1', 'state2']
    })
    config = {'model_params': {'test_size': 0.5, 'random_state': 42}}
    # Mock mlflow para não registrar nada durante o teste
    import sys
    sys.modules['mlflow'] = __import__('types')
    sys.modules['mlflow.sklearn'] = __import__('types')
    train_classification_model(df, config) 