from typing import Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum, avg, max, min
from .logger import logger

class MetricsCollector:
    @staticmethod
    def collect_product_metrics(df: DataFrame) -> Dict[str, Any]:
        """
        Coleta métricas de produtos
        """
        try:
            metrics = {
                'total_products': df.count(),
                'products_in_promotion': df.filter(col('promocao')).count(),
                'unique_categories': df.select('categoria').distinct().count(),
                'avg_price': df.select(avg('preco')).collect()[0][0],
                'max_price': df.select(max('preco')).collect()[0][0],
                'min_price': df.select(min('preco')).collect()[0][0],
                'total_stock': df.select(sum('quantidade_estoque')).collect()[0][0]
            }
            
            logger.info(f"Métricas de produtos coletadas: {metrics}")
            return metrics
        except Exception as e:
            logger.error(f"Erro ao coletar métricas de produtos: {str(e)}", exc_info=e)
            raise

    @staticmethod
    def collect_transaction_metrics(df: DataFrame) -> Dict[str, Any]:
        """
        Coleta métricas de transações
        """
        try:
            metrics = {
                'total_transactions': df.count(),
                'unique_products': df.select('produto_id').distinct().count(),
                'total_quantity': df.select(sum('quantidade')).collect()[0][0],
                'total_value': df.select(sum('valor_total')).collect()[0][0],
                'avg_transaction_value': df.select(avg('valor_total')).collect()[0][0]
            }
            
            logger.info(f"Métricas de transações coletadas: {metrics}")
            return metrics
        except Exception as e:
            logger.error(f"Erro ao coletar métricas de transações: {str(e)}", exc_info=e)
            raise 