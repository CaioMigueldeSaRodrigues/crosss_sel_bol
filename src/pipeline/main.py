from src.scraping.magalu import scrape_magalu as magalu_scraper
from src.scraping.bemol import scrape_bemol
from src.processing.cleaning import clean_dataframe_prices
from src.processing.embeddings import generate_dataframe_embeddings
from src.analysis.similarity import match_products
from src.export.export_excel import export_to_excel
from src.email.send_email import send_email
from src.logger_config import setup_logger
from src import config
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime

logger = setup_logger(__name__)

def save_to_delta(df, table_name, mode="overwrite"):
    """Salva DataFrame no formato Delta Lake"""
    full_table_name = f"bol.{table_name}"
    df.write.format("delta") \
        .mode(mode) \
        .option("mergeSchema", "true") \
        .option("overwriteSchema", "true") \
        .saveAsTable(full_table_name)
    return full_table_name


def run_full_pipeline(spark, run_date=None):
    """
    Executa o pipeline completo de scraping, limpeza, embeddings, matching, exportação e envio de e-mail.
    """
    try:
        logger.info("Iniciando scraping Magazine Luiza")
        categorias = ["Eletroportateis", "Informatica", "Tv e Video", "Moveis", "Eletrodomesticos", "Celulares"]
        df_magalu = magalu_scraper(spark=spark, categorias_a_raspar=categorias, paginas=17)
        df_magalu = df_magalu.withColumn("source", lit("magalu")).withColumn("extraction_date", current_timestamp())
        table_name = save_to_delta(df_magalu, "raw_magalu_products")
        logger.info(f"Dados do Magazine Luiza salvos em: {table_name}")

        logger.info("Obtendo dados da Bemol")
        df_bemol = scrape_bemol(spark)
        if df_bemol is None:
            raise Exception("Falha ao obter dados da Bemol")
        table_name = save_to_delta(df_bemol, "raw_bemol_products")
        logger.info(f"Dados da Bemol salvos em: {table_name}")

        logger.info("Iniciando limpeza de preços")
        df_magalu = clean_dataframe_prices(df_magalu)
        df_bemol = clean_dataframe_prices(df_bemol)
        table_name = save_to_delta(df_magalu, "processed_magalu_products")
        logger.info(f"Dados processados do Magazine Luiza salvos em: {table_name}")
        table_name = save_to_delta(df_bemol, "processed_bemol_products")
        logger.info(f"Dados processados da Bemol salvos em: {table_name}")

        logger.info("Iniciando geração de embeddings")
        df_magalu = generate_dataframe_embeddings(df_magalu, column_name="title")
        df_magalu = df_magalu.withColumnRenamed("embedding", "magalu_embedding")
        df_bemol = generate_dataframe_embeddings(df_bemol, column_name="title")
        df_bemol = df_bemol.withColumnRenamed("embedding", "bemol_embedding")

        logger.info("Iniciando matching de produtos")
        df_final = match_products(df_magalu, df_bemol, similarity_threshold=config.PROCESSING_CONFIG['similarity_threshold'])
        df_final = df_final.withColumn("matching_date", current_timestamp())
        table_name = save_to_delta(df_final, "product_matches")
        logger.info(f"Resultados do matching salvos em: {table_name}")

        logger.info("Iniciando exportação para Excel")
        excel_path = f"/dbfs/FileStore/exports/benchmarking_produtos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        export_to_excel(df_final, excel_path)
        logger.info(f"Exportação concluída. Arquivo salvo em: {excel_path}")

        if config.EMAIL_CONFIG['enabled']:
            logger.info("Enviando email com resultados")
            subject = f"Scraping - Benchmarking de produtos - {datetime.now().strftime('%Y-%m-%d')}"
            html_content = df_final.limit(20).toPandas().to_html(index=False, escape=False)
            send_email(subject=subject, html_content=html_content)
            logger.info("Email enviado com sucesso")

        logger.info(f"Pipeline finalizado com sucesso! Excel salvo em: {excel_path}")
        logger.info(f"Total de produtos pareados: {df_final.count()}")
        return excel_path
    except Exception as e:
        logger.critical(f"Erro durante a execução do pipeline: {str(e)}", exc_info=True)
        raise 