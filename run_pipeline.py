# run_pipeline.py

from pyspark.sql import SparkSession
import yaml
from src.association_rules import gerar_regras_associacao
from KPI_BI.power_bi_export import gerar_kpis_para_power_bi # Nova importação
from pyspark.sql.functions import col # Adicionado para usar col()

def main():
    """
    Orquestra o pipeline de Market Basket Analysis e gera saída para o Power BI.
    """
    spark = SparkSession.builder.appName("MarketBasketAnalysis_BI").getOrCreate()

    # Carregar configurações
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    tabelas = config['tabelas']
    filtros = config['filtros']
    caminho_saida_kpi = config['caminho_saida_kpi'] # Novo caminho do config

    # ETAPA 1: Carregar e Filtrar Dados
    faturamento = spark.table(tabelas['faturamento'])
    produtos = spark.table(tabelas['produtos'])
    faturamento_filtrado = faturamento.filter(
        (col("CENTRO") == filtros['centro']) &
        (col("QTDE") > filtros['qtde_minima']) &
        (col("CATEGORIA") == filtros['categoria'])
    )
    produtos_filtrado = produtos.filter(
        (col("CENTRO") == filtros['centro']) &
        (col("CATEGORIA") == filtros['categoria'])
    )
    faturamento_filtrado.cache() # Cache para reuso

    # ETAPA 2: Módulo de IA - Gerar Regras
    regras_ia = gerar_regras_associacao(spark, faturamento_filtrado, config)

    # ETAPA 3: Módulo de BI - Gerar KPIs
    kpis_df = gerar_kpis_para_power_bi(spark, faturamento_filtrado, regras_ia, produtos_filtrado)
    
    print("\n--- Amostra dos KPIs para Power BI (Ordenado por Lift) ---")
    kpis_df.show(20, truncate=False)

    # ETAPA 4: Salvar Saída em Parquet
    print(f"\nSalvando KPIs em formato Parquet em: {caminho_saida_kpi}")
    kpis_df.coalesce(1).write.mode("overwrite").parquet(caminho_saida_kpi.replace("/dbfs", ""))

    faturamento_filtrado.unpersist()
    print("--- Pipeline concluído com sucesso! ---")
    spark.stop()

if __name__ == "__main__":
    main() 