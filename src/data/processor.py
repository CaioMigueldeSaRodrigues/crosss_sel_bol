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
        Inicializa o processador de dados.
        As colunas requeridas aqui são baseadas na saída esperada das consultas às tabelas bol.produtos_site e bol.faturamento_centros_bol.
        """
        self.required_product_columns = [
            'id', 'nome', 'categoria', 'preco',
            'promocao', 'quantidade_estoque' # Removido 'data_atualizacao' que não está explícito na bol.produtos_site
        ]
        self.required_transaction_columns = [
            'pedido_id', 'produto_id', 'quantidade', 'valor_total', 'DT_FATURAMENTO', 'cliente_id'
        ]
        
    def process_product_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Processa os dados de produtos da tabela `bol.produtos_site`.
        
        Args:
            df: DataFrame com dados brutos de produtos.
            
        Returns:
            DataFrame de produtos processado e limpo.
        """
        if not validate_dataframe(df, self.required_product_columns):
            raise ValueError("DataFrame de produtos não possui todas as colunas necessárias")
            
        # Limpeza e normalização
        df['nome'] = df['nome'].str.strip()
        df['categoria'] = df['categoria'].str.strip()
        df['preco'] = pd.to_numeric(df['preco'], errors='coerce')
        # Ajuste para 'promocao': se PRECO_PROM existe e é menor que PRECO, é uma promoção.
        # Caso contrário, assumimos que a coluna promocao já é um booleano ou 0/1 do SQL.
        # df['promocao'] = df['promocao'].astype(bool)
        
        # Remover duplicatas baseadas no ID do produto
        df = df.drop_duplicates(subset=['id'])
        
        logger.info(f"Dados de produtos processados: {len(df)} produtos únicos.")
        return df
        
    def process_transaction_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Processa os dados de transações da tabela `bol.faturamento_centros_bol`.
        Retorna um DataFrame que pode ser usado para gerar regras de associação.
        
        Args:
            df: DataFrame com dados de transações.
            
        Returns:
            DataFrame de transações processado, agrupado por pedido.
        """
        if not validate_dataframe(df, self.required_transaction_columns):
            raise ValueError("DataFrame de transações não possui todas as colunas necessárias")
            
        # Garantir que os IDs de produto sejam strings para consistência em análises futuras
        df['produto_id'] = df['produto_id'].astype(str)
        
        # Agrupar produtos por pedido - Retorna um DataFrame para flexibilidade
        # A lista de transações (formato para Apriori) será gerada posteriormente, se necessário.
        processed_df = df.groupby('pedido_id')['produto_id'].apply(list).reset_index(name='itens_do_pedido')

        logger.info(f"Dados de transações processados: {len(processed_df)} pedidos únicos.")
        return processed_df
        
    def prepare_data_for_analysis(
        self,
        product_df: pd.DataFrame,
        transaction_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Prepara dados de produtos e transações para análises subsequentes e construção de modelos.
        Esta função substitui a lógica de mesclagem promocional e pré-treinamento.
        
        Args:
            product_df: DataFrame com dados de produtos (já processados).
            transaction_df: DataFrame com dados de transações (já processados).
            
        Returns:
            Tupla com o DataFrame de produtos processados e o DataFrame de transações processadas.
        """
        # Simplesmente retorna os DataFrames já processados pelo DataProcessor
        # A união com as regras de associação (cross_regras_varejo) será feita em etapas posteriores.
        return product_df, transaction_df 