# run_pipeline.py

import sys
import os

# --- INÍCIO DA CORREÇÃO DEFINITIVA PARA ModuleNotFoundError ---
def find_project_root():
    """Tenta encontrar a raiz do projeto de forma robusta."""
    # Método 1: Ideal para Databricks Jobs
    if 'DATABRICKS_REPOS_PATH' in os.environ:
        repo_path = os.environ['DATABRICKS_REPOS_PATH']
        repo_name = os.path.basename(os.getcwd())
        project_root = os.path.join(repo_path, repo_name)
        if os.path.exists(project_root):
            return project_root

    # Método 2: Padrão para execução de scripts .py
    try:
        project_root = os.path.dirname(os.path.abspath(__file__))
        if os.path.exists(os.path.join(project_root, 'src')):
             return project_root
    except NameError:
        pass

    # Método 3: Fallback para execução interativa
    project_root = os.getcwd()
    if os.path.exists(os.path.join(project_root, 'src')):
        return project_root
        
    return None

project_root_path = find_project_root()
if project_root_path:
    print(f"[INFO] Raiz do projeto encontrada em: {project_root_path}")
    if project_root_path not in sys.path:
        sys.path.append(project_root_path)
else:
    print("[ERRO] Não foi possível determinar a raiz do projeto. As importações podem falhar.")
# --- FIM DA CORREÇÃO ---


# Agora as importações devem funcionar
from pyspark.sql import SparkSession
import yaml
from pyspark.sql.functions import col
from src.association_rules import gerar_regras_associacao
from KPI_BI.power_bi_export import gerar_kpis_para_power_bi
from src.marketing_list import gerar_lista_de_marketing
from KPI_BI.geospatial_temporal_analysis import analisar_padroes_geograficos_temporais

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
    clientes = spark.table(tabelas['clientes'])
    
    faturamento_filtrado = faturamento.filter(
        (col("CENTRO") == filtros['centro']) &
        (col("QTDE") > filtros['qtde_minima']) &
        (col("CATEGORIA") == filtros['categoria'])
    )
    produtos_filtrado = produtos.filter(
        (col("CENTRO") == filtros['centro']) & 
        (col("CATEGORIA") == filtros['categoria'])
    )
    clientes_filtrado = clientes.filter(
        (col("FONE_1").isNotNull() | col("FONE_2").isNotNull()) &
        (col("NAO_USA").isNull())
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
    regras_ia.coalesce(1).write.mode("overwrite").parquet(caminhos['regras_associacao'].replace("/dbfs", ""))
    lista_marketing_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(caminhos['lista_marketing'].replace("/dbfs", ""))
    kpis_df.coalesce(1).write.mode("overwrite").parquet(caminhos['kpis_power_bi'].replace("/dbfs", ""))
    kpis_bairro_df.coalesce(1).write.mode("overwrite").parquet(caminhos['kpis_geograficos'].replace("/dbfs", ""))
    kpis_mes_df.coalesce(1).write.mode("overwrite").parquet(caminhos['kpis_temporais'].replace("/dbfs", ""))
    
    faturamento_filtrado.unpersist()
    print("--- Pipeline concluído com sucesso! ---")
    spark.stop()

if __name__ == "__main__":
    main() 