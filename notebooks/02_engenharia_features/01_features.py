# Databricks notebook source
# COMMAND ----------

import sys
import os
# Adiciona a raiz do projeto ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../..')))
from config import config
from src.data.processor import DataProcessor
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, count, sum, avg, desc
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline

# COMMAND ----------

# MAGIC %run "../../config/config"

# COMMAND ----------

# MAGIC %md
# MAGIC # Engenharia de Features
# MAGIC 
# MAGIC Este notebook realiza a criação e transformação de features para o sistema de recomendação.

# COMMAND ----------

# Configuração do Spark
spark = SparkSession.builder \
    .appName("Engenharia de Features") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregamento dos Dados Processados

# COMMAND ----------

# Carregar dados processados
produtos_df = spark.table("bol.produtos_processados")
transacoes_df = spark.table("bol.transacoes_processadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Features de Produtos

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Features de Popularidade

# COMMAND ----------

# Calcular features de popularidade
popularidade_df = transacoes_df.groupBy("produto_id") \
    .agg(
        count("*").alias("total_vendas"),
        sum("quantidade").alias("total_unidades"),
        sum("valor_total").alias("total_faturamento"),
        avg("valor_total").alias("ticket_medio")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Features de Categoria

# COMMAND ----------

# Indexar categorias
indexer = StringIndexer(
    inputCol="categoria",
    outputCol="categoria_index"
)

# One-hot encoding
encoder = OneHotEncoder(
    inputCol="categoria_index",
    outputCol="categoria_encoded"
)

# Pipeline de transformação
pipeline = Pipeline(stages=[indexer, encoder])
categoria_df = pipeline.fit(produtos_df).transform(produtos_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Features de Preço

# COMMAND ----------

# Calcular features de preço
preco_df = produtos_df.withColumn(
    "faixa_preco",
    when(col("preco") < 50, "baixo")
    .when(col("preco") < 200, "medio")
    .otherwise("alto")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Features de Transações

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Features de Associação

# COMMAND ----------

# Calcular co-ocorrência de produtos
co_ocorrencia_df = transacoes_df.join(
    transacoes_df,
    "pedido_id"
).where(
    col("produto_id") < col("produto_id_2")
).groupBy(
    "produto_id",
    "produto_id_2"
).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Features Temporais

# COMMAND ----------

# Calcular features temporais
temporal_df = transacoes_df.withColumn(
    "mes",
    month(col("data_pedido"))
).withColumn(
    "dia_semana",
    dayofweek(col("data_pedido"))
).groupBy(
    "produto_id",
    "mes",
    "dia_semana"
).agg(
    count("*").alias("vendas_periodo")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Mesclagem de Features

# COMMAND ----------

# Mesclar todas as features
features_df = produtos_df \
    .join(popularidade_df, "produto_id", "left") \
    .join(categoria_df, "produto_id", "left") \
    .join(preco_df, "produto_id", "left") \
    .join(temporal_df, "produto_id", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Salvamento das Features

# COMMAND ----------

# Salvar features
features_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("bol.produtos_features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validação das Features

# COMMAND ----------

# Verificar features geradas
print("Features disponíveis:")
for coluna in features_df.columns:
    print(f"- {coluna}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Próximos Passos

# COMMAND ----------

# MAGIC %md
# MAGIC As features estão prontas para serem utilizadas no próximo notebook de geração de recomendações.
# MAGIC 
# MAGIC Principais outputs:
# MAGIC - Tabela `bol.produtos_features` com todas as features geradas
# MAGIC - Tabela de co-ocorrência para regras de associação 