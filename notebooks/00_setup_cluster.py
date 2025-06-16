# Databricks notebook source
# COMMAND ----------

# MAGIC %md
# MAGIC # Configuração do Cluster para Benchmarking
# MAGIC 
# MAGIC Este notebook configura o ambiente Databricks para o pipeline de benchmarking de preços.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instalação de Bibliotecas
# MAGIC 
# MAGIC Instalando apenas as bibliotecas necessárias que não estão presentes no cluster.

# COMMAND ----------

# Verifica e instala sentence-transformers se necessário
try:
    import sentence_transformers
    print(f"sentence-transformers já instalado: v{sentence_transformers.__version__}")
except ImportError:
    print("Instalando sentence-transformers...")
    %pip install sentence-transformers==2.2.2
    dbutils.library.restartPython()

# COMMAND ----------

# Verifica e instala NLTK se necessário
try:
    import nltk
    print(f"NLTK já instalado: v{nltk.__version__}")
except ImportError:
    print("Instalando NLTK...")
    %pip install nltk==3.8.1
    dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do NLTK
# MAGIC 
# MAGIC Baixando recursos necessários do NLTK.

# COMMAND ----------

import nltk

# Baixa recursos do NLTK se necessário
try:
    nltk.data.find('tokenizers/punkt')
    print("Recursos do NLTK já configurados")
except LookupError:
    print("Baixando recursos do NLTK...")
    nltk.download('punkt')
    nltk.download('stopwords')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação de Versões
# MAGIC 
# MAGIC Verificando as versões das bibliotecas principais.

# COMMAND ----------

import pyspark
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
import nltk

print(f"PySpark: v{pyspark.__version__}")
print(f"Pandas: v{pd.__version__}")
print(f"NumPy: v{np.__version__}")
print(f"SentenceTransformer: v{sentence_transformers.__version__}")
print(f"NLTK: v{nltk.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração de Logging
# MAGIC 
# MAGIC Configurando o sistema de logging para o pipeline.

# COMMAND ----------

import logging

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Testa logging
logger = logging.getLogger(__name__)
logger.info("Ambiente configurado com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximos Passos
# MAGIC 
# MAGIC O ambiente está configurado e pronto para executar o pipeline de benchmarking.
# MAGIC 
# MAGIC Para executar o pipeline:
# MAGIC 1. Importe o módulo `benchmarking_databricks_unified`
# MAGIC 2. Execute a função `main()` 