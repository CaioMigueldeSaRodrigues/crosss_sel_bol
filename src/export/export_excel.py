import pandas as pd
import logging
from pyspark.sql import SparkSession

def export_to_excel(df, output_path):
    """
    Exporta DataFrame Spark para Excel
    
    Args:
        df (DataFrame): DataFrame Spark para exportar
        output_path (str): Caminho do arquivo Excel de saída
    """
    try:
        # Converte para pandas
        pdf = df.toPandas()
        
        # Exporta para Excel
        pdf.to_excel(output_path, index=False, engine='openpyxl')
        
        logging.info(f"Exportação para Excel concluída: {output_path}")
        
    except Exception as e:
        logging.error(f"Erro durante exportação para Excel: {str(e)}")
        raise 