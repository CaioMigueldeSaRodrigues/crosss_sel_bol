import pandas as pd
from datetime import datetime
import logging
import os
from src.config import DBFS_EXPORTS_PATH, EXPORT_CONFIG, LOGGING_CONFIG

# Configuração de logging
logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG["level"]),
    format=LOGGING_CONFIG["format"]
)
logger = logging.getLogger(__name__)

def export_to_excel(df, filename=None):
    """
    Exporta um DataFrame para um arquivo Excel.
    
    Args:
        df (pd.DataFrame): DataFrame a ser exportado
        filename (str, optional): Nome do arquivo. Se None, gera um nome baseado na data/hora.
        
    Returns:
        str: Caminho do arquivo exportado
        
    Raises:
        ValueError: Se o DataFrame for None ou vazio
        ValueError: Se as colunas configuradas não existirem no DataFrame
        IOError: Se houver erro ao criar o diretório ou salvar o arquivo
    """
    if df is None or df.empty:
        raise ValueError("DataFrame não pode ser None ou vazio")
        
    # Verifica se as colunas configuradas existem no DataFrame
    missing_columns = [col for col in EXPORT_CONFIG['excel']['columns'] if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Colunas não encontradas no DataFrame: {missing_columns}")
        
    try:
        # Gera nome do arquivo se não fornecido
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"product_matches_{timestamp}.xlsx"
            
        # Define caminho completo
        filepath = os.path.join(DBFS_EXPORTS_PATH, filename)
        
        # Cria diretório se não existir
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        # Seleciona colunas conforme configuração
        columns = EXPORT_CONFIG['excel']['columns']
        df_export = df[columns]
        
        # Exporta para Excel
        df_export.to_excel(
            filepath,
            sheet_name=EXPORT_CONFIG['excel']['sheet_name'],
            index=False
        )
        
        logger.info(f"Arquivo exportado com sucesso: {filepath}")
        return filepath
        
    except Exception as e:
        logger.error(f"Erro ao exportar para Excel: {str(e)}")
        raise 