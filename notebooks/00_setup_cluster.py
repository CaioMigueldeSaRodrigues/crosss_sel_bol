# Databricks notebook source
# COMMAND ----------

# MAGIC %md
# MAGIC # Setup do Ambiente
# MAGIC Este notebook configura o ambiente necessário para o projeto de scraping.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Instalar Bibliotecas
# MAGIC Instalando as bibliotecas necessárias no cluster.

# COMMAND ----------

# COMMAND ----------

# Instalar bibliotecas necessárias
!pip install sentence-transformers==2.2.2 pandas==2.1.4 openpyxl==3.1.2 beautifulsoup4==4.12.2 requests==2.31.0 sendgrid==6.10.0 delta-spark==3.0.0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criar Diretórios
# MAGIC Criando os diretórios necessários no DBFS.

# COMMAND ----------

# COMMAND ----------

# Criar diretórios necessários
dbutils.fs.mkdirs("/FileStore/tables/raw")
dbutils.fs.mkdirs("/FileStore/tables/processed")
dbutils.fs.mkdirs("/FileStore/tables/exports")
dbutils.fs.mkdirs("/FileStore/logs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar Permissões
# MAGIC Verificando acesso às tabelas Delta necessárias.

# COMMAND ----------

# COMMAND ----------

# Verificar acesso às tabelas
try:
    spark.sql("SELECT 1 FROM bol.feed_varejo_vtex LIMIT 1")
    print("✅ Acesso à tabela bol.feed_varejo_vtex confirmado")
except Exception as e:
    print("❌ Erro ao acessar tabela bol.feed_varejo_vtex:")
    print(str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificar Bibliotecas
# MAGIC Verificando se todas as bibliotecas foram instaladas corretamente.

# COMMAND ----------

# COMMAND ----------

# Verificar instalação das bibliotecas
import importlib

required_libraries = [
    'sentence_transformers',
    'pandas',
    'openpyxl',
    'bs4',
    'requests',
    'sendgrid',
    'delta'
]

for lib in required_libraries:
    try:
        importlib.import_module(lib)
        print(f"✅ Biblioteca {lib} instalada corretamente")
    except ImportError:
        print(f"❌ Erro ao importar {lib}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Configurar Logging
# MAGIC Configurando o sistema de logging.

# COMMAND ----------

# COMMAND ----------

import os
import logging

# Garante que o diretório existe
os.makedirs("/dbfs/FileStore/logs", exist_ok=True)

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("/dbfs/FileStore/logs/setup.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
logger.info("Setup do ambiente concluído com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximos Passos
# MAGIC 1. Reinicie o cluster para aplicar as alterações
# MAGIC 2. Execute o notebook `01_main_pipeline.py` 