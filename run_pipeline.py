# run_pipeline.py

from pyspark.sql import SparkSession
from src.market_basket import executar_analise_cesta

def main():
    """
    Orquestra o pipeline de Market Basket Analysis.
    """
    # Inicializa a sessão Spark
    spark = SparkSession.builder.appName("MarketBasketAnalysis").getOrCreate()

    # Executa a lógica principal da análise
    relatorio_df = executar_analise_cesta(spark)

    # Exibe as 20 principais recomendações no console
    print("\n--- Top 20 Recomendações de Compra Casada ---")
    relatorio_df.show(20, truncate=False)

    # Salva o relatório final no DBFS
    caminho_saida = "/dbfs/mnt/datalake/silver/benchmarking/basket_analysis_report"
    print(f"\nSalvando relatório completo em: {caminho_saida}")
    
    # Usamos o .coalesce(1) para salvar como um único arquivo, ideal para relatórios
    relatorio_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(caminho_saida.replace("/dbfs", ""))

    print("--- Pipeline concluído com sucesso! ---")
    spark.stop()


if __name__ == "__main__":
    main() 