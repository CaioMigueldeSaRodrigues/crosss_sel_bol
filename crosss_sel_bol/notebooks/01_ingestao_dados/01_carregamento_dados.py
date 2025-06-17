# Databricks notebook source
# COMMAND ----------

# MAGIC %run "../../config/config.py"

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
# As colunas foram ajustadas com base nos nomes exatos da tabela 'bol.produtos_site' fornecidos na imagem.
# Note que 'data_atualizacao' não foi encontrada nas informações fornecidas e pode precisar de ajuste ou de outra fonte.
produtos_df = spark.sql("""
    SELECT
        MATERIAL AS id,
        DESCRICAO_MATERIAL AS nome, -- Coluna 'nome' mapeada para DESCRICAO_MATERIAL
        CATEGORIA AS categoria,
        PRECO AS preco,
        PRECO_PROM AS promocao,
        QUANTIDADE_ESTOQUE AS quantidade_estoque
    FROM bol.produtos_site
    WHERE QUANTIDADE_ESTOQUE > 4
    AND CENTRO = 102
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carregamento de Dados de Transações

# COMMAND ----------

# Carregar dados de transações
# As colunas e junções foram ajustadas com base nos nomes reais e relações fornecidas.
transacoes_df = spark.sql("""
    SELECT
        f.PEDIDO AS pedido_id,
        cv.antecedent AS produto_id,
        f.QTD_ITEM AS quantidade, -- Coluna 'quantidade' mapeada para QTD_ITEM
        f.VALOR_LIQUIDO AS valor_total,
        f.DT_FATURAMENTO AS data_pedido
    FROM bol.faturamento_centros_bol f
    JOIN hive_metastore.mawe_gold.cross_varejo cv
        ON f.PEDIDO = cv.PEDIDO
    WHERE f.DT_FATURAMENTO >= current_date() - 90
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Carregamento de Dados Promocionais

# COMMAND ----------

# Carregar dados promocionais do OneDrive
from src.data.processor import DataProcessor

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
produtos_processados = processor.process_product_data(produtos_df.toPandas())
transacoes_processadas = processor.process_transaction_data(transacoes_df.toPandas())
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