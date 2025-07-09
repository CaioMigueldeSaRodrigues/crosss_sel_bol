# run_pipeline.py

from pyspark.sql import SparkSession
import yaml
from pyspark.sql.functions import col
from src.association_rules import gerar_regras_associacao
from KPI_BI.power_bi_export import gerar_kpis_para_power_bi
from src.marketing_list import gerar_lista_de_marketing
from KPI_BI.geospatial_temporal_analysis import analisar_padroes_geograficos_temporais # Nova importação

def main():
    spark = SparkSession.builder.appName("Full_Recommendation_Pipeline").getOrCreate()

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    tabelas = config['tabelas']
    filtros = config['filtros']
    caminhos = config['caminhos_saida']

    # ETAPA 1: Carregar e Filtrar Dados
    faturamento = spark.table(tabelas['faturamento'])
    produtos = spark.table(tabelas['produtos'])
    clientes = spark.table(tabelas['clientes']) # Usa o nome do config

    faturamento_filtrado = faturamento.filter(
        (col("CENTRO") == filtros['centro']) & (col("QTDE") > filtros['qtde_minima']) &
        (col("CATEGORIA") == filtros['categoria'])
    )
    produtos_filtrado = produtos.filter(
        (col("CENTRO") == filtros['centro']) & (col("CATEGORIA") == filtros['categoria'])
    )
    clientes_filtrado = clientes.filter(
        (col("FONE_1").isNotNull() | col("FONE_2").isNotNull()) &
        (col("NAO_USA").isNull()) # Novo filtro
    )
    
    faturamento_filtrado.cache()

    # ETAPA 2: Módulo de IA - Gerar Regras de Associação
    regras_ia = gerar_regras_associacao(spark, faturamento_filtrado, config)

    # ETAPA 3: Módulo de BI - Gerar KPIs de Compra Casada
    kpis_df, dados_com_clientes = gerar_kpis_para_power_bi(spark, faturamento_filtrado, regras_ia, produtos_filtrado, clientes_filtrado)

    # ETAPA 4: Gerar Lista de Marketing
    lista_marketing_df = gerar_lista_de_marketing(dados_com_clientes)

    # ETAPA 5: Gerar Análise Geográfica e Temporal
    faturamento_com_clientes = faturamento_filtrado.join(clientes_filtrado, "CLI", "inner")
    kpis_bairro_df, kpis_mes_df = analisar_padroes_geograficos_temporais(faturamento_com_clientes)

    # ETAPA 6: Salvar Todas as Saídas
    print("\n--- Salvando Produtos Finais ---")
    
    # Saída 1: Regras de Associação
    regras_ia.coalesce(1).write.mode("overwrite").parquet(caminhos['regras_associacao'].replace("/dbfs", ""))
    
    # Saída 2: Lista de Marketing
    lista_marketing_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(caminhos['lista_marketing'].replace("/dbfs", ""))

    # Saída 3: KPIs de Compra Casada para Power BI
    kpis_df.coalesce(1).write.mode("overwrite").parquet(caminhos['kpis_power_bi'].replace("/dbfs", ""))

    # Saída 4 e 5: KPIs Geográficos e Temporais para Power BI
    kpis_bairro_df.coalesce(1).write.mode("overwrite").parquet(caminhos['kpis_geograficos'].replace("/dbfs", ""))
    kpis_mes_df.coalesce(1).write.mode("overwrite").parquet(caminhos['kpis_temporais'].replace("/dbfs", ""))

    faturamento_filtrado.unpersist()
    print("--- Pipeline concluído com sucesso! ---")
    spark.stop()

if __name__ == "__main__":
    main() 