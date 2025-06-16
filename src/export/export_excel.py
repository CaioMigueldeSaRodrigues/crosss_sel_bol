import pandas as pd
from datetime import datetime
from ..config import DBFS_EXPORTS_PATH, EXPORT_CONFIG

def export_to_excel(df, filename=None):
    """
    Exporta um DataFrame para um arquivo Excel.
    
    Args:
        df (pd.DataFrame): DataFrame a ser exportado
        filename (str, optional): Nome do arquivo. Se None, gera um nome baseado na data/hora.
        
    Returns:
        str: Caminho do arquivo exportado
    """
    try:
        # Gera nome do arquivo se não fornecido
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"product_matches_{timestamp}.xlsx"
            
        # Define caminho completo
        filepath = f"{DBFS_EXPORTS_PATH}/{filename}"
        
        # Seleciona colunas conforme configuração
        columns = EXPORT_CONFIG['excel']['columns']
        df_export = df[columns]
        
        # Exporta para Excel
        df_export.to_excel(
            filepath,
            sheet_name=EXPORT_CONFIG['excel']['sheet_name'],
            index=False
        )
        
        print(f"Arquivo exportado com sucesso: {filepath}")
        return filepath
        
    except Exception as e:
        print(f"Erro ao exportar para Excel: {str(e)}")
        raise 