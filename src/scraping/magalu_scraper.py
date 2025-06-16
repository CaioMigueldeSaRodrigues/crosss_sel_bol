import requests
import pandas as pd
import time
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Inicializa sessão Spark
spark = SparkSession.builder \
    .appName("Magalu Product Scraper") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define categorias e URLs
CATEGORIAS = {
    "Eletroportateis": "https://www.magazineluiza.com.br/eletroportateis/l/ep/?page={}",
    "Informatica": "https://www.magazineluiza.com.br/informatica/l/in/?page={}",
    "Tv_e_Video": "https://www.magazineluiza.com.br/tv-e-video/l/et/?page={}",
    "Moveis": "https://www.magazineluiza.com.br/moveis/l/mo/?page={}",
    "Eletrodomesticos": "https://www.magazineluiza.com.br/eletrodomesticos/l/ed/?page={}",
    "Celulares": "https://www.magazineluiza.com.br/celulares-e-smartphones/l/te/?page={}"
}

# Configuração de retry para requests
def create_session():
    session = requests.Session()
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# Lista de User-Agents para rotação
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
]

def get_random_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.8,en-US;q=0.5,en;q=0.3",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }

def extrair_produtos(base_url, categoria_nome, paginas=17):
    """
    Extrai produtos de uma categoria específica do Magazine Luiza
    
    Args:
        base_url (str): URL base da categoria
        categoria_nome (str): Nome da categoria
        paginas (int): Número de páginas para extrair
        
    Returns:
        list: Lista de dicionários com informações dos produtos
    """
    produtos = []
    session = create_session()
    
    for pagina in range(1, paginas + 1):
        try:
            logging.info(f"[{categoria_nome}] Página {pagina} - Extraindo dados...")
            url = base_url.format(pagina)
            
            response = session.get(url, headers=get_random_headers(), timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            cards = soup.select('div[data-testid="product-card-content"]')
            
            if not cards:
                logging.warning(f"[{categoria_nome}] Nenhum produto encontrado na página {pagina}")
                continue
                
            for card in cards:
                try:
                    titulo = card.select_one('h2').text.strip()
                    preco_elem = card.select_one('p[data-testid="price-value"]')
                    preco = preco_elem.text.strip() if preco_elem else "Preço indisponível"
                    link_element = card.find_parent('a')
                    produto_url = link_element['href'] if link_element else ""
                    
                    if titulo and preco:  # Só adiciona se tiver título e preço
                        produtos.append({
                            "title": titulo,
                            "price": preco,
                            "url": produto_url,
                            "categoria": categoria_nome
                        })
                except Exception as e:
                    logging.error(f"[{categoria_nome}] Erro ao extrair produto individual: {str(e)}")
                    continue
            
            # Delay aleatório entre requisições
            time.sleep(random.uniform(1, 3))
            
        except requests.exceptions.RequestException as e:
            logging.error(f"[{categoria_nome}] Erro na requisição da página {pagina}: {str(e)}")
            time.sleep(5)  # Espera mais tempo em caso de erro
            continue
        except Exception as e:
            logging.error(f"[{categoria_nome}] Erro inesperado na página {pagina}: {str(e)}")
            continue
    
    return produtos

def main():
    # Define o schema
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("price", StringType(), True),
        StructField("url", StringType(), True),
        StructField("categoria", StringType(), True)
    ])
    
    # Loop por categoria
    for nome_categoria, url_base in CATEGORIAS.items():
        try:
            logging.info(f"Iniciando extração da categoria: {nome_categoria}")
            produtos = extrair_produtos(url_base, nome_categoria, paginas=17)
            
            if produtos:
                df_categoria = pd.DataFrame(produtos)
                spark_df = spark.createDataFrame(df_categoria, schema=schema)
                
                tabela_nome = f"bronze.magalu_{nome_categoria.lower().replace(' ', '_')}"
                spark_df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(tabela_nome)
                
                logging.info(f"✅ Tabela '{tabela_nome}' salva com sucesso. {len(produtos)} produtos extraídos.")
            else:
                logging.warning(f"[!] Nenhum produto extraído para {nome_categoria}.")
                
        except Exception as e:
            logging.error(f"Erro ao processar categoria {nome_categoria}: {str(e)}")
            continue

if __name__ == "__main__":
    main()
