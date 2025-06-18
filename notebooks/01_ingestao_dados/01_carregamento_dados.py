# Databricks notebook source
# COMMAND ----------

import sys
import os
# Adicione o caminho absoluto do seu Databricks Repos abaixo
sys.path.append('/Workspace/Repos/caio.rodrigues@bemol.com.br/crosss_sel_bol')
from config import config
from src.data.processor import DataProcessor

# COMMAND ----------

# MAGIC %run "../../config/config"

# COMMAND ----------

# MAGIC %md
# MAGIC # Carregamento de Dados
# MAGIC 
# MAGIC Este notebook realiza o carregamento e pré-processamento dos dados necessários para o sistema de recomendação.

# COMMAND ----------

# Importação de bibliotecas
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

# COMMAND ----------

# Configuração do Spark
spark = SparkSession.builder \
    .appName("Ingestão de Dados") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregamento de Dados de Produtos

# COMMAND ----------

# Carregar dados de produtos
produtos_df = spark.sql("""
    SELECT 
        id,
        nome,
        categoria,
        preco,
        promocao,
        quantidade_estoque,
        data_atualizacao
    FROM bol.produtos_site
    WHERE data_atualizacao >= current_date() - 30
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carregamento de Dados de Transações

# COMMAND ----------

# Carregar dados de transações
transacoes_df = spark.sql("""
    SELECT 
        pedido_id,
        produto_id,
        quantidade,
        valor_total,
        DT_FATURAMENTO,
        cliente_id
    FROM bol.faturamento_centros_bol
    WHERE DT_FATURAMENTO >= current_date() - 90
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Carregamento de Dados Promocionais

# COMMAND ----------

# Carregar dados promocionais do OneDrive

# Configurar acesso ao OneDrive
access_token = dbutils.secrets.get(scope="onedrive", key="access_token")

# Carregar planilha de promoções
promocoes_df = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "'COMERCIAL_(AM_RR)'!A1") \
    .load(PROMOTION_CONFIG['excel_path'])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Processamento de Dados

# COMMAND ----------

# Inicializar processador
processor = DataProcessor()

# Processar dados
produtos_pd = produtos_df.toPandas()
transacoes_pd = transacoes_df.toPandas()

produtos_processados = processor.process_product_data(produtos_pd)
transacoes_processadas = processor.process_transaction_data(transacoes_pd)
promocoes_processadas = processor.process_promotional_data(promocoes_df.toPandas())

# Mesclar dados
dados_finais = processor.merge_data(produtos_processados, promocoes_processadas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Salvamento dos Dados Processados

# COMMAND ----------

# Converter para DataFrame Spark
dados_finais_spark = spark.createDataFrame(dados_finais)

# Salvar dados processados
dados_finais_spark.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("bol.produtos_processados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validação dos Dados

# COMMAND ----------

# Verificar quantidade de produtos
print(f"Total de produtos: {dados_finais_spark.count()}")
print(f"Produtos em promoção: {dados_finais_spark.filter(col('promocao')).count()}")
print(f"Categorias únicas: {dados_finais_spark.select('categoria').distinct().count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Próximos Passos

# COMMAND ----------

# MAGIC %md
# MAGIC Os dados processados estão prontos para serem utilizados no próximo notebook de engenharia de features.
# MAGIC 
# MAGIC Principais outputs:
# MAGIC - Tabela `bol.produtos_processados` com dados de produtos e promoções
# MAGIC - Lista de transações processadas para geração de regras de associação 

# COMMAND ----------

# (Opcional) Salvar resultados processados em Delta Lake
produtos_spark = spark.createDataFrame(produtos_processados)
produtos_spark.write.mode("overwrite").format("delta").saveAsTable("bol.produtos_processados")

transacoes_spark = spark.createDataFrame(transacoes_processadas)
transacoes_spark.write.mode("overwrite").format("delta").saveAsTable("bol.transacoes_processadas")

# COMMAND ----------

# Chamar subnotebooks (exemplo: engenharia de features, recomendações)
# %run ../02_engenharia_features/01_features
# %run ../03_recomendacoes/01_geracao_recomendacoes 