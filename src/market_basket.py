# src/market_basket.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
from src.association_rules import gerar_regras_associacao # Importação adicionada

def executar_analise_cesta(spark: SparkSession, config):
    """
    Executa o pipeline de análise de cesta de compras (market basket analysis).
    """
    print("--- Iniciando Análise de Cesta de Compras com Módulo de IA ---")

    # 1. Carregar Tabelas e Aplicar Filtros
    print("[ETAPA 1/4] Carregando e filtrando tabelas...")
    tabelas = config['tabelas']
    filtros = config['filtros']

    faturamento = spark.table(tabelas['faturamento'])
    produtos = spark.table(tabelas['produtos'])

    faturamento_filtrado = faturamento.filter(
        (col("CENTRO") == filtros['centro']) &
        (col("QTDE") > filtros['qtde_minima']) &
        (col("CATEGORIA") == filtros['categoria'])
    )
    produtos_filtrado = produtos.filter(
        (col("CENTRO") == filtros['centro']) & (col("CATEGORIA") == filtros['categoria'])
    )
    
    # 2. MÓDULO DE IA: Gerar novas regras de associação
    # Usamos o faturamento já filtrado para treinar o modelo
    regras_ia = gerar_regras_associacao(spark, faturamento_filtrado, config)
    
    # Renomear colunas para evitar conflito no join
    regras = regras_ia.withColumnRenamed("antecedent_ia", "antecedent") \
                      .withColumnRenamed("consequent_ia", "consequent")

    # 3. Unir Tabelas com as novas regras geradas pela IA
    print("[ETAPA 3/4] Unindo dados com as regras da IA...")
    dados_analise = regras.join(
        faturamento_filtrado,
        regras.antecedent[0] == faturamento_filtrado.MATERIAL, # FP-Growth retorna arrays
        "inner"
    )
    
    # 4. Calcular Métricas para o Relatório
    print("[ETAPA 4/4] Calculando métricas de suporte e confiança...")
    suporte_antecedente = dados_analise.groupBy("antecedent").agg(
        count("*").alias("numero_compras_do_item")
    )
    suporte_regra = dados_analise.groupBy("antecedent", "consequent").agg(
        count("*").alias("numero_compras_da_combinacao")
    )
    relatorio = suporte_regra.join(suporte_antecedente, "antecedent")
    relatorio = relatorio.withColumn(
        "confianca_calculada",
        (col("numero_compras_da_combinacao") / col("numero_compras_do_item")) * 100
    )
    
    produtos_antecedent = produtos_filtrado.select(col("MATERIAL").alias("antecedent_id"), col("DESCRICAO_MATERIAL").alias("produto"))
    produtos_consequent = produtos_filtrado.select(col("MATERIAL").alias("consequent_id"), col("DESCRICAO_MATERIAL").alias("combinacao_de_itens"))
    
    relatorio_final = relatorio.join(produtos_antecedent, relatorio.antecedent[0] == produtos_antecedent.antecedent_id)
    relatorio_final = relatorio_final.join(produtos_consequent, relatorio.consequent[0] == produtos_consequent.consequent_id)

    relatorio_final = relatorio_final.select(
        "produto", "combinacao_de_itens", "numero_compras_do_item",
        "numero_compras_da_combinacao", col("confianca_calculada").alias("% Nas compras do item individual")
    ).orderBy(col("% Nas compras do item individual").desc())

    print("--- Análise de Cesta de Compras Concluída ---")
    return relatorio_final 