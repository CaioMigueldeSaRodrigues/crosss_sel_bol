from typing import Dict, List, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan
from .logger import logger

class DataValidator:
    @staticmethod
    def validate_product_data(df: DataFrame) -> Dict[str, Any]:
        """
        Valida dados de produtos
        """
        try:
            # Verificar valores nulos
            null_counts = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) 
                                   for c in df.columns])
            
            # Verificar valores negativos em campos numéricos
            negative_counts = df.select([
                count(when(col(c) < 0, c)).alias(c)
                for c in ['preco', 'promocao', 'quantidade_estoque']
            ])
            
            # Verificar categorias vazias
            empty_categories = df.filter(col('categoria').isNull() | 
                                      (col('categoria') == '')).count()
            
            return {
                'null_counts': null_counts.collect()[0].asDict(),
                'negative_counts': negative_counts.collect()[0].asDict(),
                'empty_categories': empty_categories
            }
        except Exception as e:
            logger.error(f"Erro na validação de dados de produtos: {str(e)}", exc_info=e)
            raise

    @staticmethod
    def validate_transaction_data(df: DataFrame) -> Dict[str, Any]:
        """
        Valida dados de transações
        """
        try:
            # Verificar valores nulos
            null_counts = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) 
                                   for c in df.columns])
            
            # Verificar valores negativos
            negative_counts = df.select([
                count(when(col(c) < 0, c)).alias(c)
                for c in ['quantidade', 'valor_total']
            ])
            
            # Verificar datas inválidas
            invalid_dates = df.filter(col('DT_FATURAMENTO').isNull()).count()
            
            return {
                'null_counts': null_counts.collect()[0].asDict(),
                'negative_counts': negative_counts.collect()[0].asDict(),
                'invalid_dates': invalid_dates
            }
        except Exception as e:
            logger.error(f"Erro na validação de dados de transações: {str(e)}", exc_info=e)
            raise 