# Databricks notebook source
# COMMAND ----------

# MAGIC %md
# MAGIC # Configuração do Cluster para Benchmarking
# MAGIC 
# MAGIC Este notebook configura o ambiente Databricks para o pipeline de benchmarking de preços, garantindo compatibilidade das bibliotecas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instalação e Reinstalação de Bibliotecas Essenciais
# MAGIC 
# MAGIC Desinstalando versões existentes e instalando as mais recentes compatíveis para evitar conflitos.

# COMMAND ----------

# Desinstala explicitamente para evitar qualquer conflito residual
# O '-y' é para responder 'yes' automaticamente a qualquer prompt de desinstalação
%pip uninstall -y sentence-transformers huggingface_hub nltk

# Instala a versão mais recente do sentence-transformers (que é compatível com versões mais novas do huggingface_hub)
%pip install --upgrade --force-reinstall sentence-transformers==2.7.0

# Instala NLTK
%pip install --upgrade --force-reinstall nltk==3.8.1

# Reinicia o Python para que as novas instalações tenham efeito
# Isso é crucial para que o ambiente de execução use as bibliotecas recém-instaladas
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do NLTK
# MAGIC 
# MAGIC Baixando recursos necessários do NLTK após garantir que a biblioteca esteja instalada.

# COMMAND ----------

import nltk

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
# MAGIC Verificando as versões das bibliotecas principais instaladas para diagnóstico.

# COMMAND ----------

import pyspark
import pandas as pd
import numpy as np

# Importações com tratamento de erro para garantir que o notebook não falhe aqui
try:
    from sentence_transformers import SentenceTransformer
    sbert_version = SentenceTransformer.__version__
except ImportError:
    sbert_version = "Não instalado ou com erro"

try:
    import nltk
    nltk_version = nltk.__version__
except ImportError:
    nltk_version = "Não instalado ou com erro"

try:
    import huggingface_hub
    hf_hub_version = huggingface_hub.__version__
except ImportError:
    hf_hub_version = "Não instalado ou com erro"

print(f"PySpark: v{pyspark.__version__}")
print(f"Pandas: v{pd.__version__}")
print(f"NumPy: v{np.__version__}")
print(f"huggingface_hub: v{hf_hub_version}")
print(f"SentenceTransformer: v{sbert_version}")
print(f"NLTK: v{nltk_version}")


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