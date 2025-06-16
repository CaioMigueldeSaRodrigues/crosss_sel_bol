# Databricks notebook source
# COMMAND ----------

# MAGIC %run ../src/config

# COMMAND ----------

# MAGIC %run ../src/scraping/magalu

# COMMAND ----------

# MAGIC %run ../src/scraping/bemol

# COMMAND ----------

# MAGIC %run ../src/processing/cleaning

# COMMAND ----------

# MAGIC %run ../src/processing/embeddings

# COMMAND ----------

# MAGIC %run ../src/analysis/similarity

# COMMAND ----------

# MAGIC %run ../src/export/export_excel

# COMMAND ----------

# MAGIC %run ../src/email/send_email

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime

# COMMAND ----------

def save_to_delta(df, table_name, mode="overwrite"):
    """Salva DataFrame no formato Delta Lake"""
    full_table_name = f"bol.{table_name}"
    df.write.format("delta") \
        .mode(mode) \
        .option("mergeSchema", "true") \
        .saveAsTable(full_table_name)
    return full_table_name

# COMMAND ----------

try:
    # 2. Scraping Magalu
    print("Iniciando scraping Magazine Luiza")
    df_magalu = scrape_magalu(SCRAPING_CONFIG['magalu']['categories'], paginas=2, spark=spark)
    df_magalu = df_magalu.withColumn("source", lit("magalu")) \
                        .withColumn("extraction_date", current_timestamp())
    
    # Salva dados brutos
    table_name = save_to_delta(df_magalu, "raw_magalu_products")
    print(f"Dados do Magazine Luiza salvos em: {table_name}")
    
    # 3. Obter dados da Bemol
    print("Obtendo dados da Bemol")
    df_bemol = scrape_bemol(spark)
    if df_bemol is None:
        raise Exception("Falha ao obter dados da Bemol")
    
    # Salva dados brutos
    table_name = save_to_delta(df_bemol, "raw_bemol_products")
    print(f"Dados da Bemol salvos em: {table_name}")
    
    # 4. Limpeza de preços
    print("Iniciando limpeza de preços")
    df_magalu = clean_dataframe_prices(df_magalu, col="price")
    df_bemol = clean_dataframe_prices(df_bemol, col="price")
    
    # Salva dados processados
    table_name = save_to_delta(df_magalu, "processed_magalu_products")
    print(f"Dados processados do Magazine Luiza salvos em: {table_name}")
    
    table_name = save_to_delta(df_bemol, "processed_bemol_products")
    print(f"Dados processados da Bemol salvos em: {table_name}")
    
    # 5. Geração de embeddings
    print("Iniciando geração de embeddings")
    df_magalu = generate_embeddings(df_magalu, col="title", batch_size=PROCESSING_CONFIG['batch_size'])
    df_bemol = generate_embeddings(df_bemol, col="title", batch_size=PROCESSING_CONFIG['batch_size'])
    
    # 6. Análise de similaridade e pareamento
    print("Iniciando matching de produtos")
    df_final = match_products(df_magalu, df_bemol, similarity_threshold=PROCESSING_CONFIG['similarity_threshold'])
    df_final = df_final.withColumn("matching_date", current_timestamp())
    
    # Salva resultados do matching
    table_name = save_to_delta(df_final, "product_matches")
    print(f"Resultados do matching salvos em: {table_name}")
    
    # 7. Exportação para Excel
    print("Iniciando exportação para Excel")
    excel_path = f"/dbfs/FileStore/exports/benchmarking_produtos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    export_to_excel(df_final, excel_path)
    print(f"Exportação concluída. Arquivo salvo em: {excel_path}")
    
    # 8. Envio de e-mail
    if EMAIL_CONFIG['enabled']:
        print("Enviando email com resultados")
        subject = f"Scraping - Benchmarking de produtos - {datetime.now().strftime('%Y-%m-%d')}"
        html_content = df_final.limit(20).toPandas().to_html(index=False, escape=False)
        send_email_with_attachment(
            subject=subject,
            html_content=html_content,
            attachment_path=excel_path,
            recipients=EMAIL_CONFIG['recipients'],
            bcc=EMAIL_CONFIG['bcc']
        )
        print("Email enviado com sucesso")
    
    print(f"Pipeline finalizado com sucesso!")
    print(f"Excel salvo em: {excel_path}")
    print(f"Total de produtos pareados: {df_final.count()}")
    
except Exception as e:
    print(f"Erro durante a execução do pipeline: {str(e)}")
    raise 