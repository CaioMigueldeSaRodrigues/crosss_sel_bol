import requests
from bs4 import BeautifulSoup
import time
import logging
import pandas as pd

def extrair_produtos(url, headers, paginas=2):
    """
    Extrai produtos de uma página do Magazine Luiza
    
    Args:
        url (str): URL da página de produtos
        headers (dict): Headers para a requisição HTTP
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

def scrape_magalu(categorias=None, paginas=2):
    """
    Realiza o scraping de produtos do Magazine Luiza usando pandas
    """
    base_url = "https://www.magazineluiza.com.br"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    if categorias is None:
        categorias = [
            "eletroportateis/l/ep",
            "informatica/l/in",
            "tv-e-video/l/et",
            "moveis/l/mo",
            "eletrodomesticos/l/ed",
            "celulares-e-smartphones/l/te"
        ]
    all_products = []
    for categoria in categorias:
        logging.info(f"Coletando produtos da categoria: {categoria}")
        url = f"{base_url}/{categoria}"
        produtos = extrair_produtos(url, headers, paginas)
        all_products.extend(produtos)
        logging.info(f"Coletados {len(produtos)} produtos da categoria {categoria}")
    df = pd.DataFrame(all_products)
    logging.info(f"Total de produtos coletados: {len(df)}")
    return df 