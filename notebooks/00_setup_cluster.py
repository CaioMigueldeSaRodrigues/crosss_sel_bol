# Databricks notebook source
# COMMAND ----------

# MAGIC %md
# MAGIC # Setup do Ambiente
# MAGIC Este notebook configura o ambiente necessário para o projeto de scraping.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Instalar Bibliotecas
# MAGIC Instalando apenas a biblioteca sentence-transformers que não está no cluster.

# COMMAND ----------

# Instalar apenas sentence-transformers
%pip install sentence-transformers==2.2.2

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criar Diretórios
# MAGIC Criando os diretórios necessários no DBFS.

# COMMAND ----------

# Criar diretórios necessários
dbutils.fs.mkdirs("/FileStore/tables/raw")
dbutils.fs.mkdirs("/FileStore/tables/processed")
dbutils.fs.mkdirs("/FileStore/tables/exports")
dbutils.fs.mkdirs("/FileStore/logs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar Bibliotecas Existentes
# MAGIC Verificando as versões das bibliotecas já instaladas no cluster.

# COMMAND ----------

# Verificar versões das bibliotecas principais
import pandas as pd
import numpy as np
import sklearn
import requests
import bs4
import openpyxl
import delta
import mlflow
import rich

print("Versões das bibliotecas principais:")
print(f"pandas: {pd.__version__}")
print(f"numpy: {np.__version__}")
print(f"scikit-learn: {sklearn.__version__}")
print(f"requests: {requests.__version__}")
print(f"beautifulsoup4: {bs4.__version__}")
print(f"openpyxl: {openpyxl.__version__}")
print(f"mlflow: {mlflow.__version__}")
print(f"rich: {rich.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Configurar Logging
# MAGIC Configurando o sistema de logging.

# COMMAND ----------

import logging

# Configurar logging apenas para o console (Databricks salva logs do notebook automaticamente)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
logger.info("Setup do ambiente concluído com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificar Configurações do Cluster
# MAGIC Verificando se o cluster está configurado corretamente.

# COMMAND ----------

# Verificar configurações do Spark
print("Configurações do Spark:")
print(f"Spark Version: {spark.version}")
print(f"Max Result Size: {spark.conf.get('spark.driver.maxResultSize')}")
print(f"Delta Preview: {spark.conf.get('spark.databricks.delta.preview.enabled')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verificar Acesso ao DBFS
# MAGIC Verificando se temos acesso aos diretórios do DBFS.

# COMMAND ----------

# Verificar acesso ao DBFS
try:
    dbutils.fs.ls("/FileStore/tables")
    print("✅ Acesso ao DBFS confirmado")
except Exception as e:
    print("❌ Erro ao acessar DBFS:")
    print(str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximos Passos
# MAGIC 1. Reinicie o cluster para aplicar as alterações
# MAGIC 2. Execute o notebook `01_main_pipeline.py` 