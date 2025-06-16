from pyspark.sql.functions import col, regexp_replace, when, lit
import logging
import re

def limpar_preco(preco):
    try:
        preco_str = str(preco)
        preco_str = preco_str.replace('\xa0', ' ').replace('R$', '').replace('ou', '').strip()
        match = re.search(r'(\d{1,3}(?:\.\d{3})*,\d{2})', preco_str)
        if match:
            preco_str = match.group(1).replace('.', '').replace(',', '.')
            return float(preco_str)
        preco_str = preco_str.replace('.', '').replace(',', '.')
        return float(preco_str)
    except:
        return 0.0

def clean_dataframe_prices(df, column_name="price"):
    """
    Limpa e padroniza os preços no DataFrame
    
    Args:
        df (DataFrame): DataFrame Spark com os dados
        column_name (str): Nome da coluna de preços
        
    Returns:
        DataFrame: DataFrame com preços limpos
    """
    try:
        # Remove caracteres não numéricos exceto ponto e vírgula
        df = df.withColumn(
            column_name,
            regexp_replace(col(column_name), r'[^\\d.,]', '')
        )
        
        # Substitui vírgula por ponto para decimal
        df = df.withColumn(
            column_name,
            regexp_replace(col(column_name), ',', '.')
        )
        
        # Converte para double
        df = df.withColumn(
            column_name,
            col(column_name).cast('double')
        )
        
        # Remove valores nulos ou negativos
        df = df.withColumn(
            column_name,
            when(col(column_name).isNull() | (col(column_name) <= 0), None).otherwise(col(column_name))
        )
        
        logging.info("Limpeza de preços concluída com sucesso")
        return df
        
    except Exception as e:
        logging.error(f"Erro durante a limpeza de preços: {str(e)}")
        return df 