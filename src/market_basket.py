# src/market_basket.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit

def executar_analise_cesta(spark: SparkSession):
    """
    Executa o pipeline de análise de cesta de compras (market basket analysis).

    1. Carrega as tabelas do Hive Metastore.
    2. Aplica filtros de negócio.
    3. Une as tabelas para enriquecer as regras.
    4. Calcula as métricas de suporte e confiança.
    5. Retorna o relatório final.

    Args:
        spark (SparkSession): A sessão Spark ativa.

    Returns:
        DataFrame: Um DataFrame do Spark com a análise final.
    """
    print("--- Iniciando Análise de Cesta de Compras ---")

    # 1. Carregar Tabelas
    print("[ETAPA 1/4] Carregando tabelas do Hive Metastore...")
    regras = spark.table("hive_metastore.mawe_gold.cross_regras_varejo")
    faturamento = spark.table("bol.faturamento_centros_bol")
    produtos = spark.table("bol.produtos_site")

    # 2. Aplicar Filtros de Negócio
    print("[ETAPA 2/4] Aplicando filtros de negócio...")
    faturamento_filtrado = faturamento.filter(
        (col("CENTRO") == 102) &
        (col("QTDE") > 3) &
        (col("CATEGORIA") == "VAREJO")
    )

    produtos_filtrado = produtos.filter(
        (col("CENTRO") == 102) &
        (col("CATEGORIA") == "VAREJO")
    )
    
    # 3. Unir Tabelas
    print("[ETAPA 3/4] Unindo e enriquecendo as regras...")
    # Join entre as regras e o faturamento pelo produto antecedente
    dados_analise = regras.join(
        faturamento_filtrado,
        regras.antecedent == faturamento_filtrado.MATERIAL,
        "inner"
    )

    # 4. Calcular Métricas para o Relatório
    print("[ETAPA 4/4] Calculando métricas de suporte e confiança...")
    
    # Contar a frequência de cada antecedente (suporte do antecedente)
    suporte_antecedente = dados_analise.groupBy("antecedent").agg(
        count("*").alias("numero_compras_do_item")
    )

    # Contar a frequência da combinação (suporte da regra)
    suporte_regra = dados_analise.groupBy("antecedent", "consequent").agg(
        count("*").alias("numero_compras_da_combinacao")
    )

    # Unir os suportes para calcular a confiança
    relatorio = suporte_regra.join(suporte_antecedente, "antecedent")

    # Calcular a confiança
    relatorio = relatorio.withColumn(
        "confianca",
        (col("numero_compras_da_combinacao") / col("numero_compras_do_item")) * 100
    )
    
    # Adicionar informações dos produtos para o relatório final
    # Usando alias para evitar colunas ambíguas
    produtos_antecedent = produtos_filtrado.select(
        col("MATERIAL").alias("antecedent_id"), col("DESCRICAO_MATERIAL").alias("produto")
    )
    produtos_consequent = produtos_filtrado.select(
        col("MATERIAL").alias("consequent_id"), col("DESCRICAO_MATERIAL").alias("combinacao_de_itens")
    )
    
    relatorio_final = relatorio.join(produtos_antecedent, relatorio.antecedent == produtos_antecedent.antecedent_id)
    relatorio_final = relatorio_final.join(produtos_consequent, relatorio.consequent == produtos_consequent.consequent_id)

    # Selecionar e ordenar as colunas para o formato final
    relatorio_final = relatorio_final.select(
        "produto",
        "combinacao_de_itens",
        "numero_compras_do_item",
        "numero_compras_da_combinacao",
        col("confianca").alias("% Nas compras do item individual")
    ).orderBy(col("% Nas compras do item individual").desc())

    print("--- Análise de Cesta de Compras Concluída ---")
    return relatorio_final 