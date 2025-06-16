import requests
from bs4 import BeautifulSoup
import time
import logging
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType # Importar tipos Spark

# Define categorias e URLs (manter para mapeamento)
CATEGORIAS_MAP = {
    "Eletroportateis": "https://www.magazineluiza.com.br/eletroportateis/l/ep/?page={}",
    "Informatica": "https://www.magazineluiza.com.br/informatica/l/in/?page={}",
    "Tv e Video": "https://www.magazineluiza.com.br/tv-e-video/l/et/?page={}",
    "Moveis": "https://www.magazineluiza.com.br/moveis/l/mo/?page={}",
    "Eletrodomesticos": "https://www.magazineluiza.com.br/eletrodomesticos/l/ed/?page={}",
    "Celulares": "https://www.magazineluiza.com.br/celulares-e-smartphones/l/te/?page={}"
}

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extrair_produtos(base_url_template, categoria_nome, paginas=17): # Ajustado paginas para 17
    produtos = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for pagina in range(1, paginas + 1):
        logging.info(f"[{categoria_nome}] Página {pagina} - Extraindo dados...")
        url = base_url_template.format(pagina)
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            cards = soup.select('div[data-testid="product-card-content"]')
            
            if not cards:
                logging.warning(f"[{categoria_nome}] Nenhuns cartões de produto encontrados na página {pagina}. A URL pode estar incorreta ou a estrutura HTML mudou. URL: {url}")

            for card in cards:
                try:
                    title_elem = card.select_one('h2')
                    title = title_elem.text.strip() if title_elem else "Título indisponível"

                    price_elem = card.select_one('p[data-testid="price-value"]')
                    price_str = price_elem.text.strip() if price_elem else None
                    price = None
                    if price_str:
                        try:
                            # Ajuste para lidar com preços como R$ 1.234,56
                            price_clean = price_str.replace('R$', '').replace('.', '').replace(',', '.').strip()
                            price = float(price_clean)
                        except ValueError:
                            logging.warning(f"[{categoria_nome}] Não foi possível converter o preço '{price_str}' para float.")

                    link_element = card.find_parent('a')
                    product_relative_url = link_element['href'] if link_element else ""
                    product_url = f"https://www.magazineluiza.com.br{product_relative_url}" if product_relative_url.startswith('/') else product_relative_url

                    produtos.append({
                        'title': title,
                        'price': price,
                        'url': product_url,
                        'source': 'magalu',
                        'extraction_date': datetime.now().isoformat()
                    })
                except Exception as e:
                    logging.error(f"[{categoria_nome}] Erro ao extrair dados de um produto na página {pagina}: {e}")
                    continue
            
        except requests.exceptions.RequestException as e:
            logging.error(f"[{categoria_nome}] Erro ao acessar a página {pagina}: {e}. URL: {url}")
            
        time.sleep(1)

    return produtos

def scrape_magalu(spark: SparkSession, categorias_a_raspar=None, paginas=17): # Corrigindo a assinatura para incluir SparkSession
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
                logging.warning(f"Categoria '{cat_name}' não encontrada no mapeamento. Ignorando.")

    for categoria_nome, url_template in categorias_para_iterar:
        logging.info(f"Iniciando coleta de produtos da categoria: {categoria_nome}")
        # Usar paginas do argumento da função scrape_magalu
        produtos_da_categoria = extrair_produtos(url_template, categoria_nome, paginas)
        final_products_list.extend(produtos_da_categoria)
        logging.info(f"Coletados {len(produtos_da_categoria)} produtos da categoria {categoria_nome}")

    df_pandas = pd.DataFrame(final_products_list)
    if df_pandas.empty:
        logging.warning("Nenhum produto coletado para o Magazine Luiza.")
        return spark.createDataFrame([], schema=StructType([])) # Retorna um DataFrame Spark vazio com schema vazio

    # Define o schema para o Spark DataFrame
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("price", StringType(), True),
        StructField("url", StringType(), True),
        StructField("source", StringType(), True),
        StructField("extraction_date", StringType(), True) # Pode ser TimestampType se o formato isoformat for compatível
    ])

    spark_df_final = spark.createDataFrame(df_pandas, schema=schema)

    logging.info(f"Total de produtos coletados do Magazine Luiza: {spark_df_final.count()} e convertidos para Spark DataFrame.")
    return spark_df_final 