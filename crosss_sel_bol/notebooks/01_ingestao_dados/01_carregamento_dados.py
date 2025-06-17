# Databricks notebook source
# COMMAND ----------

# MAGIC %run "../../config/config.py"

# COMMAND ----------

# MAGIC %md
# MAGIC # Carregamento de Dados
# MAGIC 
# MAGIC Este notebook realiza o carregamento e pré-processamento dos dados brutos das tabelas `bol.produtos_site` e `bol.faturamento_centros_bol`.
# MAGIC A tabela `hive_metastore.mawe_gold.cross_regras_varejo` será utilizada em etapas posteriores do pipeline para geração de recomendações.

# COMMAND ----------

# Importação de bibliotecas
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, current_date
from src.utils.logger import logger
from src.utils.validators import DataValidator
from src.utils.metrics import MetricsCollector

# COMMAND ----------

# Configuração do Spark com otimizações
spark = SparkSession.builder \
    .appName("Ingestão de Dados") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "10g") \
    .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregamento de Dados de Produtos (`bol.produtos_site`)

# COMMAND ----------

try:
    # Carregar dados de produtos do bol.produtos_site com cache
    produtos_df = spark.sql("""
        SELECT
            MATERIAL AS id,
            DESCRICAO_MATERIAL AS nome,
            CATEGORIA AS categoria,
            PRECO AS preco,
            PRECO_PROM AS promocao,
            QUANTIDADE_ESTOQUE AS quantidade_estoque
        FROM bol.produtos_site
        WHERE QUANTIDADE_ESTOQUE > 4
        AND CENTRO = 102
    """).cache()
    
    # Validar dados de produtos
    validation_results = DataValidator.validate_product_data(produtos_df)
    logger.info(f"Resultados da validação de produtos: {validation_results}")
    
    # Coletar métricas de produtos
    metrics = MetricsCollector.collect_product_metrics(produtos_df)
    
except Exception as e:
    logger.error("Erro no carregamento de dados de produtos da tabela bol.produtos_site", exc_info=e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carregamento de Dados de Transações (`bol.faturamento_centros_bol`)

# COMMAND ----------

try:
    # Carregar dados de transações do bol.faturamento_centros_bol com cache
    # Esta tabela contém os itens de cada pedido.
    transacoes_df = spark.sql("""
        SELECT
            PEDIDO AS pedido_id,
            MATERIAL AS produto_id,
            QTD_ITEM AS quantidade,
            VALOR_LIQUIDO AS valor_total,
            DT_FATURAMENTO AS data_pedido,
            CLI AS cliente_id -- Adicionado campo CLI como cliente_id
        FROM bol.faturamento_centros_bol
        WHERE DT_FATURAMENTO >= current_date() - 90
    """).cache()
    
    # Validar dados de transações
    validation_results = DataValidator.validate_transaction_data(transacoes_df)
    logger.info(f"Resultados da validação de transações: {validation_results}")
    
    # Coletar métricas de transações
    metrics = MetricsCollector.collect_transaction_metrics(transacoes_df)
    
except Exception as e:
    logger.error("Erro no carregamento de dados de transações da tabela bol.faturamento_centros_bol", exc_info=e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Processamento de Dados

# COMMAND ----------

from src.data.processor import DataProcessor # Mantém importação aqui para evitar problemas de dependência se DataProcessor ainda usa promocoes_processadas

try:
    # Inicializar processador
    processor = DataProcessor()
    
    # Processar dados de produtos e transações.
    # Nota: A integração de dados promocionais e regras de associação (cross_regras_varejo)
    # será tratada em notebooks subsequentes (ex: Engenharia de Features ou Recomendações).
    with spark.sparkContext.setJobGroup("processamento_dados", "Processamento de dados brutos para recomendação"):
        produtos_processados = processor.process_product_data(produtos_df.toPandas())
        transacoes_processadas = processor.process_transaction_data(transacoes_df.toPandas())
        
        # A mesclagem com dados promocionais não ocorre nesta etapa,
        # pois a fonte original (OneDrive Excel) não está entre as 3 tabelas base.
        # Ajustar a variável final para refletir apenas dados de produtos processados ou
        # definir um novo esquema de mesclagem se a informação promocional
        # for incorporada em bol.produtos_site ou bol.faturamento_centros_bol em outra etapa.
        # Por enquanto, 'dados_finais' conterá apenas os produtos processados.
        dados_finais = produtos_processados # Assumindo que produtos_processados é a base para a próxima etapa.
    
except Exception as e:
    logger.error("Erro no processamento de dados", exc_info=e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Salvamento dos Dados Processados (`bol.produtos_processados`)

# COMMAND ----------

try:
    # Converter para DataFrame Spark
    dados_finais_spark = spark.createDataFrame(dados_finais)
    
    # Salvar dados de produtos processados na tabela bol.produtos_processados
    dados_finais_spark.write \
        .mode("overwrite") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .option("mergeSchema", "true") \
        .saveAsTable("bol.produtos_processados")
    
    logger.info("Dados de produtos processados salvos com sucesso na tabela bol.produtos_processados")
    
except Exception as e:
    logger.error("Erro ao salvar dados de produtos processados na tabela bol.produtos_processados", exc_info=e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validação Final dos Dados Processados

# COMMAND ----------

try:
    # Verificar quantidade de produtos processados
    total_produtos = dados_finais_spark.count()
    # A verificação de "Produtos em promoção" foi removida, pois dados promocionais não são mesclados aqui.
    categorias_unicas = dados_finais_spark.select('categoria').distinct().count()
    
    logger.info(f"""
    Validação Final dos Dados de Produtos Processados:
    - Total de produtos: {total_produtos}
    - Categorias únicas: {categorias_unicas}
    """)
    
except Exception as e:
    logger.error("Erro na validação final dos dados processados", exc_info=e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Limpeza de Recursos

# COMMAND ----------

# Liberar cache dos DataFrames originais
produtos_df.unpersist()
transacoes_df.unpersist()
# promocoes_df.unpersist() # Removido, pois dados promocionais não são carregados aqui.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Próximos Passos
# MAGIC 
# MAGIC Os dados brutos de produtos e transações foram carregados e pré-processados, e os produtos processados foram salvos na tabela `bol.produtos_processados`.
# MAGIC 
# MAGIC As tabelas `bol.produtos_processados` e `bol.faturamento_centros_bol` (para transações) estão prontas para serem utilizadas. A tabela `hive_metastore.mawe_gold.cross_regras_varejo` será utilizada nos próximos notebooks de engenharia de features e geração de recomendações para construir as regras de associação.
# MAGIC 
# MAGIC Principais outputs desta etapa:
# MAGIC - Tabela `bol.produtos_processados` com dados de produtos limpos e padronizados.
# MAGIC - `transacoes_df` (DataFrame em memória) com transações brutas prontas para processamento em outras etapas. 