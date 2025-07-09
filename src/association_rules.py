# src/association_rules.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list
from pyspark.ml.fpm import FPGrowth

def gerar_regras_associacao(spark: SparkSession, faturamento_df, min_support=0.01, min_confidence=0.1):
    """
    Usa o algoritmo FP-Growth para minerar regras de associação a partir dos dados de faturamento.

    Args:
        spark (SparkSession): A sessão Spark ativa.
        faturamento_df (DataFrame): DataFrame de faturamento, já filtrado.
        min_support (float): O suporte mínimo para um itemset ser considerado frequente.
        min_confidence (float): A confiança mínima para uma regra ser gerada.

    Returns:
        DataFrame: Um DataFrame com as novas regras geradas (antecedent, consequent, confidence).
    """
    print("--- Iniciando Módulo de IA: Geração de Regras de Associação ---")

    # 1. Preparar os dados: Agrupar itens por pedido para formar as "cestas"
    print("[IA] Agrupando itens por pedido para formar cestas...")
    cestas_df = faturamento_df.groupBy("PEDIDO").agg(
        collect_list("MATERIAL").alias("items")
    )

    # 2. Treinar o modelo FP-Growth
    print(f"[IA] Treinando modelo FP-Growth com min_support={min_support} e min_confidence={min_confidence}...")
    fp_growth = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence)
    model = fp_growth.fit(cestas_df)

    # 3. Extrair as regras de associação
    print("[IA] Extraindo regras de associação do modelo...")
    regras_geradas = model.associationRules

    # Renomear colunas para o padrão do projeto e selecionar o necessário
    regras_finais = regras_geradas.select(
        col("antecedent").alias("antecedent_ia"),
        col("consequent").alias("consequent_ia"),
        col("confidence")
    )

    print(f"--- Módulo de IA concluído. {regras_finais.count()} novas regras geradas. ---")
    return regras_finais 