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
# Usando versões compatíveis com o runtime 16.2.x
!pip install sentence-transformers==2.2.2
!pip install pandas==2.1.4
!pip install openpyxl==3.1.2
!pip install beautifulsoup4==4.12.2
!pip install requests==2.31.0
!pip install sendgrid==6.10.0
!pip install delta-spark==3.0.0
!pip install scikit-learn==1.3.2
!pip install databricks-sql-connector==2.9.3
!pip install sqlalchemy==2.0.23
!pip install pyhive==0.7.0
!pip install mlflow==2.8.1
!pip install rich==13.7.0

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
    'delta',
    'sklearn',
    'databricks',
    'sqlalchemy',
    'pyhive',
    'mlflow',
    'rich'
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
# MAGIC ## 6. Verificar Configurações do Cluster
# MAGIC Verificando se o cluster está configurado corretamente.

# COMMAND ----------

# COMMAND ----------

# Verificar configurações do Spark
print("Configurações do Spark:")
print(f"Spark Version: {spark.version}")
print(f"Driver Memory: {spark.conf.get('spark.driver.memory')}")
print(f"Max Result Size: {spark.conf.get('spark.driver.maxResultSize')}")
print(f"Delta Preview: {spark.conf.get('spark.databricks.delta.preview.enabled')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verificar Acesso ao DBFS
# MAGIC Verificando se temos acesso aos diretórios do DBFS.

# COMMAND ----------

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