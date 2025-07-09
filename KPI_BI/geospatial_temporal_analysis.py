# KPI_BI/geospatial_temporal_analysis.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum as _sum, avg, date_format

def analisar_padroes_geograficos_temporais(faturamento_com_clientes: DataFrame):
    """
    Gera KPIs agregados por bairro e por mês para identificar padrões.

    Args:
        faturamento_com_clientes (DataFrame): DataFrame de faturamento unido com dados de clientes.

    Returns:
        (DataFrame, DataFrame): Tupla contendo (kpis_por_bairro, kpis_por_mes).
    """
    print("--- Gerando KPIs Geográficos e Temporais ---")

    # 1. Análise por Bairro
    kpis_por_bairro = faturamento_com_clientes.groupBy("BAIRRO").agg(
        countDistinct("CLI").alias("total_clientes_unicos"),
        _sum("VALOR_LIQUIDO").alias("faturamento_total"),
        avg("VALOR_LIQUIDO").alias("ticket_medio")
    ).orderBy(col("faturamento_total").desc())

    # 2. Análise por Mês
    faturamento_com_mes = faturamento_com_clientes.withColumn(
        "ano_mes", date_format(col("DT_FATURAMENTO"), "yyyy-MM")
    )
    kpis_por_mes = faturamento_com_mes.groupBy("ano_mes").agg(
        countDistinct("PEDIDO").alias("total_pedidos"),
        _sum("VALOR_LIQUIDO").alias("faturamento_total")
    ).orderBy("ano_mes")
    
    print("--- KPIs Geográficos e Temporais gerados com sucesso ---")
    return kpis_por_bairro, kpis_por_mes
