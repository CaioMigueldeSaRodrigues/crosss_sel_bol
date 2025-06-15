import requests
from bs4 import BeautifulSoup
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, lit

def extrair_produtos(url, headers, spark, paginas=2):
    """
    Extrai produtos de uma página do Magazine Luiza
    
    Args:
        url (str): URL da página de produtos
        headers (dict): Headers para a requisição HTTP
        spark (SparkSession): Sessão Spark
        paginas (int): Número de páginas para extrair
        
    Returns:
        list: Lista de dicionários com informações dos produtos
    """
    produtos = []
    
    for pagina in range(1, paginas + 1):
        try:
            url_pagina = f"{url}?page={pagina}"
            response = requests.get(url_pagina, headers=headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            items = soup.find_all('a', {'data-testid': 'product-card-container'})
            
            for item in items:
                try:
                    title = item.find('h2', {'data-testid': 'product-title'}).text.strip()
                    price_elem = item.find('p', {'data-testid': 'price-value'})
                    price = float(price_elem.text.strip().replace('R$', '').replace('.', '').replace(',', '.')) if price_elem else None
                    url_produto = 'https://www.magazineluiza.com.br' + item['href']
                    
                    produtos.append({
                        'title': title,
                        'price': price,
                        'url': url_produto
                    })
                except Exception as e:
                    logging.error(f"Erro ao extrair produto: {str(e)}")
                    continue
                    
            time.sleep(1)  # Respeitar rate limiting
            
        except Exception as e:
            logging.error(f"Erro ao acessar página {pagina}: {str(e)}")
            continue
            
    return produtos

def scrape_magalu(categorias, spark, paginas=2):
    """
    Realiza o scraping de produtos do Magazine Luiza
    
    Args:
        categorias (list): Lista de categorias para scraping
        spark (SparkSession): Sessão Spark
        paginas (int): Número de páginas por categoria
        
    Returns:
        DataFrame: DataFrame Spark com os produtos coletados
    """
    base_url = "https://www.magazineluiza.com.br"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("url", StringType(), True)
    ])
    
    all_products = []
    
    for categoria in categorias:
        logging.info(f"Coletando produtos da categoria: {categoria}")
        url = f"{base_url}/{categoria}"
        produtos = extrair_produtos(url, headers, spark, paginas)
        all_products.extend(produtos)
        logging.info(f"Coletados {len(produtos)} produtos da categoria {categoria}")
        
    # Criar DataFrame Spark
    df = spark.createDataFrame(all_products, schema)
    logging.info(f"Total de produtos coletados: {df.count()}")
    
    return df 