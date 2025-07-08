"""
Funções de pré-processamento de dados para o projeto de recomendação/cross-sell.
Adicione aqui funções reutilizáveis de limpeza, transformação e validação de dados.
"""

import pandas as pd
from typing import Any, Optional

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza nomes de colunas para minúsculas e sem espaços.
    """
    df = df.copy()
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    return df

# Exemplo de função de limpeza de preços
def clean_price_column(df: pd.DataFrame, price_col: str = 'preco') -> pd.DataFrame:
    """
    Remove caracteres não numéricos da coluna de preço e converte para float.
    """
    df = df.copy()
    df[price_col] = (
        df[price_col]
        .astype(str)
        .str.replace(r'[^0-9,.]', '', regex=True)
        .str.replace(',', '.', regex=False)
        .astype(float)
    )
    return df

# Adicione outras funções de pré-processamento abaixo 

def preprocessar_dados_cliente(df_clientes, df_pedidos):
    """
    Função de exemplo para unir e limpar os dados.
    Adapte com a sua lógica real.
    """
    # Exemplo: Unir os dataframes
    df_completo = pd.merge(df_clientes, df_pedidos, on='customer_id')
    
    # Exemplo: Tratar valores nulos
    df_completo.fillna(0, inplace=True)
    
    print("Dados pré-processados com sucesso!")
    return df_completo 