"""
Processamento de dados para o sistema de recomendação
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple
from ..utils.helpers import setup_logging, validate_dataframe

logger = setup_logging(__name__)

class DataProcessor:
    def __init__(self):
        """
        Inicializa o processador de dados
        """
        self.required_columns = [
            'id', 'nome', 'categoria', 'preco',
            'promocao', 'data_atualizacao'
        ]
        
    def process_product_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Processa os dados de produtos
        
        Args:
            df: DataFrame com dados brutos
            
        Returns:
            DataFrame processado
        """
        if not validate_dataframe(df, self.required_columns):
            raise ValueError("DataFrame não possui todas as colunas necessárias")
            
        # Limpeza e normalização
        df['nome'] = df['nome'].str.strip()
        df['categoria'] = df['categoria'].str.strip()
        df['preco'] = pd.to_numeric(df['preco'], errors='coerce')
        df['promocao'] = df['promocao'].astype(bool)
        
        # Remover duplicatas
        df = df.drop_duplicates(subset=['id'])
        
        logger.info(f"Dados processados: {len(df)} produtos")
        return df
        
    def process_transaction_data(self, df: pd.DataFrame) -> List[List[str]]:
        """
        Processa os dados de transações
        
        Args:
            df: DataFrame com dados de transações
            
        Returns:
            Lista de transações
        """
        required_cols = ['pedido_id', 'produto_id']
        if not validate_dataframe(df, required_cols):
            raise ValueError("DataFrame não possui todas as colunas necessárias")
            
        # Agrupar produtos por pedido
        transactions = df.groupby('pedido_id')['produto_id'].apply(list).tolist()
        
        logger.info(f"Transações processadas: {len(transactions)} pedidos")
        return transactions
        
    def process_promotional_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Processa os dados promocionais
        
        Args:
            df: DataFrame com dados promocionais
            
        Returns:
            DataFrame processado
        """
        required_cols = ['id', 'nome', 'preco_original', 'preco_promocional']
        if not validate_dataframe(df, required_cols):
            raise ValueError("DataFrame não possui todas as colunas necessárias")
            
        # Calcular desconto
        df['desconto'] = (
            (df['preco_original'] - df['preco_promocional']) /
            df['preco_original'] * 100
        )
        
        # Ordenar por desconto
        df = df.sort_values('desconto', ascending=False)
        
        logger.info(f"Dados promocionais processados: {len(df)} produtos")
        return df
        
    def merge_data(
        self,
        product_df: pd.DataFrame,
        promo_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Mescla dados de produtos e promoções
        
        Args:
            product_df: DataFrame com dados de produtos
            promo_df: DataFrame com dados promocionais
            
        Returns:
            DataFrame mesclado
        """
        # Mesclar dados
        merged_df = pd.merge(
            product_df,
            promo_df,
            on='id',
            how='left',
            suffixes=('', '_promo')
        )
        
        # Atualizar preços promocionais
        merged_df['preco'] = np.where(
            merged_df['preco_promocional'].notna(),
            merged_df['preco_promocional'],
            merged_df['preco']
        )
        
        # Atualizar flag de promoção
        merged_df['promocao'] = merged_df['preco_promocional'].notna()
        
        logger.info(f"Dados mesclados: {len(merged_df)} produtos")
        return merged_df
        
    def prepare_training_data(
        self,
        product_df: pd.DataFrame,
        transaction_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, List[List[str]]]:
        """
        Prepara dados para treinamento do modelo
        
        Args:
            product_df: DataFrame com dados de produtos
            transaction_df: DataFrame com dados de transações
            
        Returns:
            Tupla com DataFrame de produtos e lista de transações
        """
        # Processar dados
        processed_products = self.process_product_data(product_df)
        processed_transactions = self.process_transaction_data(transaction_df)
        
        return processed_products, processed_transactions 