import requests
from bs4 import BeautifulSoup
import time
import logging
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from src.config import SCRAPING_CONFIG, LOGGING_CONFIG
import re

# Configuração de logging
logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG["level"]),
    format=LOGGING_CONFIG["format"]
)
logger = logging.getLogger(__name__)

# Define categorias e URLs (manter para mapeamento)
CATEGORIAS_MAP = {
    "Eletroportateis": "https://www.magazineluiza.com.br/eletroportateis/l/ep/?page={}",
    "Informatica": "https://www.magazineluiza.com.br/informatica/l/in/?page={}",
    "Tv e Video": "https://www.magazineluiza.com.br/tv-e-video/l/et/?page={}",
    "Moveis": "https://www.magazineluiza.com.br/moveis/l/mo/?page={}",
    "Eletrodomesticos": "https://www.magazineluiza.com.br/eletrodomesticos/l/ed/?page={}",
    "Celulares": "https://www.magazineluiza.com.br/celulares-e-smartphones/l/te/?page={}"
}

def create_session():
    """Cria uma sessão HTTP com retry mechanism."""
    session = requests.Session()
    retry_strategy = Retry(
        total=SCRAPING_CONFIG["magalu"]["max_retries"],
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def extrair_produtos(base_url_template, categoria_nome, paginas=17):
    produtos = []
    session = create_session()
    headers = SCRAPING_CONFIG["magalu"]["headers"]

    for pagina in range(1, paginas + 1):
        logger.info(f"[{categoria_nome}] Página {pagina} - Extraindo dados...")
        url = base_url_template.format(pagina)
        try:
            response = session.get(
                url, 
                headers=headers,
                timeout=SCRAPING_CONFIG["magalu"]["timeout"]
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            cards = soup.select('div[data-testid="product-card-content"]')
            
            if not cards:
                logger.warning(f"[{categoria_nome}] Nenhuns cartões de produto encontrados na página {pagina}. A URL pode estar incorreta ou a estrutura HTML mudou. URL: {url}")
                continue

            for card in cards:
                try:
                    title_elem = card.select_one('h2')
                    title = title_elem.text.strip() if title_elem else "Título indisponível"

                    # Extrair preço
                    preco_element = card.find('p', {'data-testid': 'price-value'})
                    if preco_element:
                        preco_texto = preco_element.text.strip()
                        
                        try:
                            # Extrai apenas os números e a vírgula
                            numeros = re.findall(r'[\d,]+', preco_texto)
                            if numeros:
                                # Pega o primeiro número encontrado
                                preco_texto = numeros[0]
                                # Remove pontos de milhar e substitui vírgula por ponto
                                preco_texto = preco_texto.replace('.', '').replace(',', '.')
                                preco = float(preco_texto)
                            else:
                                preco = None
                        except (ValueError, AttributeError) as e:
                            preco = None
                    else:
                        logger.warning(f"[{categoria_nome}] Elemento de preço não encontrado")
                        preco = None

                    link_element = card.find_parent('a')
                    product_relative_url = link_element['href'] if link_element else ""
                    product_url = f"https://www.magazineluiza.com.br{product_relative_url}" if product_relative_url.startswith('/') else product_relative_url

                    produtos.append({
                        'title': title,
                        'price': preco,
                        'url': product_url,
                        'source': 'magalu',
                        'extraction_date': datetime.now()
                    })
                except Exception as e:
                    logger.error(f"[{categoria_nome}] Erro ao extrair dados de um produto na página {pagina}: {e}")
                    continue
            
            # Rate limiting
            time.sleep(1)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"[{categoria_nome}] Erro ao acessar a página {pagina}: {e}. URL: {url}")
            continue

    return produtos

def scrape_magalu(spark: SparkSession, categorias_a_raspar=None, paginas=17):
    """
    Realiza o scraping de produtos do Magazine Luiza e retorna um Spark DataFrame.
    
    Args:
        spark (SparkSession): Sessão Spark ativa.
        categorias_a_raspar (list, optional): Uma lista de nomes de categorias a serem raspadas.
                                              Se None, raspa um conjunto padrão de categorias.
                                              Ex: ["Eletroportateis", "Celulares"]
        paginas (int): Número de páginas para extrair por categoria. Default é 17.
        
    Returns:
        pyspark.sql.DataFrame: DataFrame Spark com os produtos raspados.
    """
    final_products_list = []
    
    if categorias_a_raspar is None:
        categorias_para_iterar = CATEGORIAS_MAP.items()
    else:
        categorias_para_iterar = []
        for cat_name in categorias_a_raspar:
            if cat_name in CATEGORIAS_MAP:
                categorias_para_iterar.append((cat_name, CATEGORIAS_MAP[cat_name]))
            else:
                logger.warning(f"Categoria '{cat_name}' não encontrada no mapeamento. Ignorando.")

    for categoria_nome, url_template in categorias_para_iterar:
        logger.info(f"Iniciando coleta de produtos da categoria: {categoria_nome}")
        produtos_da_categoria = extrair_produtos(url_template, categoria_nome, paginas)
        final_products_list.extend(produtos_da_categoria)
        logger.info(f"Coletados {len(produtos_da_categoria)} produtos da categoria {categoria_nome}")

    df_pandas = pd.DataFrame(final_products_list)
    if df_pandas.empty:
        logger.warning("Nenhum produto coletado para o Magazine Luiza.")
        return spark.createDataFrame([], schema=StructType([]))

    # Define o schema para o Spark DataFrame
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("url", StringType(), True),
        StructField("source", StringType(), True),
        StructField("extraction_date", TimestampType(), True)
    ])

    spark_df_final = spark.createDataFrame(df_pandas, schema=schema)
    logger.info(f"Total de produtos coletados do Magazine Luiza: {spark_df_final.count()} e convertidos para Spark DataFrame.")
    return spark_df_final 