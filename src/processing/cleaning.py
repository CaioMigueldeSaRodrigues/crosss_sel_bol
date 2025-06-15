from pyspark.sql.functions import col, regexp_replace, when, lit
import logging

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

def clean_dataframe_prices(df, col="price"):
    """
    Limpa e padroniza os preços no DataFrame
    
    Args:
        df (DataFrame): DataFrame Spark com os dados
        col (str): Nome da coluna de preços
        
    Returns:
        DataFrame: DataFrame com preços limpos
    """
    try:
        # Remove caracteres não numéricos exceto ponto e vírgula
        df = df.withColumn(
            col,
            regexp_replace(col(col), r'[^\d.,]', '')
        )
        
        # Substitui vírgula por ponto para decimal
        df = df.withColumn(
            col,
            regexp_replace(col(col), ',', '.')
        )
        
        # Converte para double
        df = df.withColumn(
            col,
            col(col).cast('double')
        )
        
        # Remove valores nulos ou negativos
        df = df.withColumn(
            col,
            when(col(col).isNull() | (col(col) <= 0), None).otherwise(col(col))
        )
        
        logging.info("Limpeza de preços concluída com sucesso")
        return df
        
    except Exception as e:
        logging.error(f"Erro durante a limpeza de preços: {str(e)}")
        return df 