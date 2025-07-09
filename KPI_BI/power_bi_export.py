# KPI_BI/power_bi_export.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, lit

def gerar_kpis_para_power_bi(spark: SparkSession, faturamento_filtrado: DataFrame, regras_ia: DataFrame, produtos_filtrado: DataFrame):
    """
    Gera a tabela de KPIs otimizada para o Power BI, incluindo a métrica de Lift.

    Args:
        spark (SparkSession): A sessão Spark ativa.
        faturamento_filtrado (DataFrame): DataFrame de faturamento já filtrado.
        regras_ia (DataFrame): DataFrame com as regras geradas pelo FP-Growth.
        produtos_filtrado (DataFrame): DataFrame com os produtos já filtrados.

    Returns:
        DataFrame: Um DataFrame do Spark com os KPIs finais.
    """
    print("--- Gerando KPIs para o Power BI ---")

    # Renomear colunas das regras para o join
    regras = regras_ia.withColumnRenamed("antecedent", "antecedent_ia") \
                      .withColumnRenamed("consequent", "consequent_ia")

    # 1. CALCULAR SUPORTES
    total_transacoes = faturamento_filtrado.select("PEDIDO").distinct().count()

    # Suporte do Antecedente
    suporte_antecedente = faturamento_filtrado.groupBy(col("MATERIAL").alias("antecedent")) \
        .agg(count("PEDIDO").alias("numero_compras_do_item"))

    # Suporte do Consequente (necessário para o Lift)
    suporte_consequente = faturamento_filtrado.groupBy(col("MATERIAL").alias("consequent")) \
        .agg((count("PEDIDO") / total_transacoes).alias("suporte_consequente"))

    # Suporte da Regra (antecedent + consequent)
    dados_analise = regras.join(
        faturamento_filtrado,
        regras.antecedent_ia[0] == faturamento_filtrado.MATERIAL,
        "inner"
    )
    suporte_regra = dados_analise.groupBy("antecedent_ia", "consequent_ia").agg(
        count("*").alias("numero_compras_da_combinacao")
    )

    # 2. UNIR MÉTRICAS E CALCULAR KPIs
    relatorio = suporte_regra.join(suporte_antecedente, suporte_regra.antecedent_ia[0] == suporte_antecedente.antecedent)
    relatorio = relatorio.withColumn("confianca", col("numero_compras_da_combinacao") / col("numero_compras_do_item"))
    relatorio = relatorio.join(suporte_consequente, relatorio.consequent_ia[0] == suporte_consequente.consequent)
    
    # Cálculo do Lift
    relatorio = relatorio.withColumn("lift", col("confianca") / col("suporte_consequente"))

    # 3. ENRIQUECER COM DESCRIÇÕES DOS PRODUTOS
    produtos_desc = produtos_filtrado.select("MATERIAL", "DESCRICAO_MATERIAL")
    
    relatorio_final = relatorio.join(produtos_desc, relatorio.antecedent_ia[0] == produtos_desc.MATERIAL) \
        .withColumnRenamed("DESCRICAO_MATERIAL", "produto") \
        .drop("MATERIAL")
        
    relatorio_final = relatorio_final.join(produtos_desc, relatorio.consequent_ia[0] == produtos_desc.MATERIAL) \
        .withColumnRenamed("DESCRICAO_MATERIAL", "combinacao_de_itens") \
        .drop("MATERIAL")

    # 4. SELECIONAR E FORMATAR COLUNAS FINAIS
    relatorio_power_bi = relatorio_final.select(
        col("antecedent_ia")[0].alias("id_produto"),
        col("produto"),
        col("consequent_ia")[0].alias("id_combinacao"),
        col("combinacao_de_itens"),
        col("numero_compras_do_item"),
        col("numero_compras_da_combinacao"),
        (col("confianca") * 100).alias("% Confianca"),
        col("lift").alias("Lift (x vezes mais provável)")
    ).orderBy(col("lift").desc())
    
    print("--- KPIs para Power BI gerados com sucesso ---")
    return relatorio_power_bi
