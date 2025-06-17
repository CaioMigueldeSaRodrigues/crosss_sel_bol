# Databricks notebook source
# COMMAND ----------

# MAGIC %run "../../config/config"

# COMMAND ----------

# MAGIC %md
# MAGIC # Geração de Recomendações
# MAGIC 
# MAGIC Este notebook gera as recomendações de produtos usando diferentes estratégias.

# COMMAND ----------

# Importação de bibliotecas
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, count, sum, avg, desc
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from src.models.recommender import ProductRecommender
import sys
sys.path.append("../../config")
import config

# COMMAND ----------

# Configuração do Spark
spark = SparkSession.builder \
    .appName("Geração de Recomendações") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregamento dos Dados

# COMMAND ----------

# Carregar dados
produtos_df = spark.table("bol.produtos_features")
transacoes_df = spark.table("bol.faturamento_centros_bol")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Inicialização do Recomendador

# COMMAND ----------

# Inicializar recomendador
recommender = ProductRecommender(RECOMMENDATION_CONFIG)

# Carregar dados
recommender.load_data(produtos_df.toPandas())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Geração de Recomendações

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Produtos Similares

# COMMAND ----------

# Construir matriz de similaridade
recommender.build_similarity_matrix()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Cross-Selling

# COMMAND ----------

# Gerar regras de associação
transactions = transacoes_df.groupBy("pedido_id") \
    .agg(collect_list("produto_id").alias("produtos")) \
    .select("produtos") \
    .collect()

transaction_list = [row.produtos for row in transactions]
recommender.generate_association_rules(transaction_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Produtos em Promoção

# COMMAND ----------

# Obter produtos em promoção
promocoes = recommender.get_promotional_recommendations()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Exemplo de Recomendações

# COMMAND ----------

# Selecionar produto de exemplo
produto_exemplo = produtos_df.first()

# Gerar recomendações
recomendacoes = recommender.get_recommendations(produto_exemplo.id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Produtos Similares

# COMMAND ----------

# Exibir produtos similares
display(pd.DataFrame(recomendacoes['similares']))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Cross-Selling

# COMMAND ----------

# Exibir cross-selling
display(pd.DataFrame(recomendacoes['cross_selling']))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Promoções

# COMMAND ----------

# Exibir promoções
display(pd.DataFrame(recomendacoes['promocionais']))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Salvamento das Recomendações

# COMMAND ----------

# Converter recomendações para DataFrame
recomendacoes_df = spark.createDataFrame(
    pd.DataFrame([
        {
            'produto_id': produto_exemplo.id,
            'tipo': tipo,
            'produto_recomendado': rec['id'],
            'score': rec['score']
        }
        for tipo, recs in recomendacoes.items()
        for rec in recs
    ])
)

# Salvar recomendações
recomendacoes_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("bol.recomendacoes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validação das Recomendações

# COMMAND ----------

# Verificar quantidade de recomendações
print(f"Total de recomendações: {recomendacoes_df.count()}")
print(f"Recomendações por tipo:")
recomendacoes_df.groupBy("tipo").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Próximos Passos

# COMMAND ----------

# MAGIC %md
# MAGIC As recomendações foram geradas e salvas na tabela `bol.recomendacoes`.
# MAGIC 
# MAGIC Principais outputs:
# MAGIC - Tabela `bol.recomendacoes` com todas as recomendações geradas
# MAGIC - Exemplos de recomendações para diferentes tipos 