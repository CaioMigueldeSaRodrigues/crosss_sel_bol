"""
Módulo unificado para benchmarking de preços no Databricks.
Integra scraping, processamento de dados, matching de similaridade e geração de relatórios.
"""

import os
import re
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, udf, lit, when, regexp_replace, 
    concat, split, explode, array, struct
)
from pyspark.sql.types import (
    StringType, DoubleType, ArrayType, 
    StructType, StructField
)
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import requests
from bs4 import BeautifulSoup
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MagaluScraper:
    """Classe responsável pelo scraping de produtos do Magazine Luiza."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.base_url = "https://www.magazineluiza.com.br"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
    def extrair_produtos(self, termo_busca: str, max_paginas: int = 5) -> DataFrame:
        """
        Extrai produtos do Magazine Luiza e retorna um DataFrame Spark.
        
        Args:
            termo_busca: Termo de busca para os produtos
            max_paginas: Número máximo de páginas para buscar
            
        Returns:
            DataFrame Spark com os produtos extraídos
        """
        try:
            produtos = []
            for pagina in range(1, max_paginas + 1):
                url = f"{self.base_url}/busca/{termo_busca}/?page={pagina}"
                response = requests.get(url, headers=self.headers)
                
                if response.status_code != 200:
                    logger.warning(f"Erro ao acessar página {pagina}: {response.status_code}")
                    continue
                
                soup = BeautifulSoup(response.text, 'html.parser')
                items = soup.find_all('a', {'data-testid': 'product-card-container'})
                
                for item in items:
                    try:
                        nome = item.find('h2', {'data-testid': 'product-title'}).text.strip()
                        preco_texto = item.find('p', {'data-testid': 'price-value'}).text.strip()
                        preco = self._limpar_preco(preco_texto)
                        url_produto = self.base_url + item['href']
                        
                        produtos.append({
                            'nome': nome,
                            'preco': preco,
                            'url': url_produto,
                            'loja': 'Magazine Luiza'
                        })
                    except Exception as e:
                        logger.error(f"Erro ao extrair produto: {str(e)}")
                        continue
            
            return self.spark.createDataFrame(produtos)
            
        except Exception as e:
            logger.error(f"Erro no scraping: {str(e)}")
            return self.spark.createDataFrame([])
    
    def _limpar_preco(self, preco_texto: str) -> float:
        """
        Limpa e converte o texto do preço para float.
        
        Args:
            preco_texto: Texto do preço a ser limpo
            
        Returns:
            Preço como float
        """
        try:
            # Remove caracteres especiais e converte vírgula para ponto
            preco_limpo = re.sub(r'[^\d,.]', '', preco_texto)
            preco_limpo = preco_limpo.replace(',', '.')
            
            # Verifica se há mais de um ponto decimal
            if preco_limpo.count('.') > 1:
                partes = preco_limpo.split('.')
                preco_limpo = '.'.join(partes[:-1]) + '.' + partes[-1]
            
            return float(preco_limpo)
        except Exception as e:
            logger.error(f"Erro ao limpar preço '{preco_texto}': {str(e)}")
            return 0.0

class DataProcessor:
    """Classe responsável pelo processamento e limpeza dos dados."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._configurar_nltk()
    
    def _configurar_nltk(self):
        """Configura o NLTK com os recursos necessários."""
        try:
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            nltk.download('punkt')
        try:
            nltk.data.find('corpora/stopwords')
        except LookupError:
            nltk.download('stopwords')
    
    def processar_dados(self, df: DataFrame) -> DataFrame:
        """
        Processa e limpa os dados do DataFrame.
        
        Args:
            df: DataFrame com os dados brutos
            
        Returns:
            DataFrame processado
        """
        try:
            # Limpa preços
            df = df.withColumn(
                'preco_limpo',
                udf(self._limpar_preco, DoubleType())(col('preco'))
            )
            
            # Limpa nomes
            df = df.withColumn(
                'nome_limpo',
                udf(self._limpar_nome, StringType())(col('nome'))
            )
            
            # Gera IDs únicos
            df = df.withColumn(
                'id',
                concat(
                    lit('ML_'),
                    regexp_replace(col('nome_limpo'), r'[^a-zA-Z0-9]', ''),
                    lit('_'),
                    regexp_replace(col('preco_limpo').cast(StringType()), r'[^0-9]', '')
                )
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Erro no processamento de dados: {str(e)}")
            return df
    
    def _limpar_preco(self, preco: str) -> float:
        """Limpa e converte preço para float."""
        try:
            if isinstance(preco, (int, float)):
                return float(preco)
            
            preco_limpo = re.sub(r'[^\d,.]', '', str(preco))
            preco_limpo = preco_limpo.replace(',', '.')
            
            if preco_limpo.count('.') > 1:
                partes = preco_limpo.split('.')
                preco_limpo = '.'.join(partes[:-1]) + '.' + partes[-1]
            
            return float(preco_limpo)
        except Exception as e:
            logger.error(f"Erro ao limpar preço '{preco}': {str(e)}")
            return 0.0
    
    def _limpar_nome(self, nome: str) -> str:
        """Limpa e normaliza o nome do produto."""
        try:
            # Converte para minúsculas
            nome = nome.lower()
            
            # Remove caracteres especiais
            nome = re.sub(r'[^\w\s]', ' ', nome)
            
            # Remove espaços extras
            nome = ' '.join(nome.split())
            
            return nome
        except Exception as e:
            logger.error(f"Erro ao limpar nome '{nome}': {str(e)}")
            return nome

class SimilarityMatcher:
    """Classe responsável pelo cálculo de similaridade e matching de produtos."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
    
    def calcular_similaridade(self, df: DataFrame) -> DataFrame:
        """
        Calcula a similaridade entre produtos e identifica pares comparáveis.
        
        Args:
            df: DataFrame com os produtos processados
            
        Returns:
            DataFrame com os pares de produtos similares
        """
        try:
            # Converte para Pandas para processamento mais eficiente
            pdf = df.toPandas()
            
            # Gera embeddings
            embeddings = self.model.encode(pdf['nome_limpo'].tolist())
            
            # Calcula matriz de similaridade
            similarity_matrix = cosine_similarity(embeddings)
            
            # Identifica pares similares
            pares_similares = []
            for i in range(len(pdf)):
                for j in range(i + 1, len(pdf)):
                    if similarity_matrix[i][j] > 0.7:  # Threshold de similaridade
                        pares_similares.append({
                            'id_1': pdf.iloc[i]['id'],
                            'id_2': pdf.iloc[j]['id'],
                            'similaridade': similarity_matrix[i][j],
                            'preco_1': pdf.iloc[i]['preco_limpo'],
                            'preco_2': pdf.iloc[j]['preco_limpo']
                        })
            
            # Converte de volta para Spark DataFrame
            return self.spark.createDataFrame(pares_similares)
            
        except Exception as e:
            logger.error(f"Erro no cálculo de similaridade: {str(e)}")
            return self.spark.createDataFrame([])
    
    def classificar_similaridade(self, similaridade: float) -> str:
        """
        Classifica o nível de similaridade entre produtos.
        
        Args:
            similaridade: Valor de similaridade (0-1)
            
        Returns:
            Classificação da similaridade
        """
        if similaridade >= 0.9:
            return "Muito Similar"
        elif similaridade >= 0.8:
            return "Similar"
        elif similaridade >= 0.7:
            return "Moderadamente Similar"
        else:
            return "Pouco Similar"
    
    def calcular_diferenca_preco(self, preco_1: float, preco_2: float) -> float:
        """
        Calcula a diferença percentual entre preços.
        
        Args:
            preco_1: Primeiro preço
            preco_2: Segundo preço
            
        Returns:
            Diferença percentual
        """
        try:
            if preco_1 == 0 or preco_2 == 0:
                return 0.0
            return abs((preco_1 - preco_2) / min(preco_1, preco_2)) * 100
        except Exception as e:
            logger.error(f"Erro ao calcular diferença de preço: {str(e)}")
            return 0.0

class ReportGenerator:
    """Classe responsável pela geração de relatórios e exportação de dados."""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def gerar_relatorio(self, df_pares: DataFrame, df_produtos: DataFrame) -> None:
        """
        Gera relatório HTML e exporta dados para DBFS.
        
        Args:
            df_pares: DataFrame com os pares similares
            df_produtos: DataFrame com os produtos originais
        """
        try:
            # Cria TempView para consultas
            df_pares.createOrReplaceTempView("pares_similares")
            df_produtos.createOrReplaceTempView("produtos")
            
            # Exporta para DBFS
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"/dbfs/FileStore/benchmarking/resultados_{timestamp}"
            
            df_pares.write.mode("overwrite").parquet(output_path)
            
            # Gera relatório HTML
            self._gerar_html_report(df_pares, df_produtos, output_path)
            
            logger.info(f"Relatório gerado com sucesso em: {output_path}")
            
        except Exception as e:
            logger.error(f"Erro na geração do relatório: {str(e)}")
    
    def _gerar_html_report(self, df_pares: DataFrame, df_produtos: DataFrame, output_path: str) -> None:
        """Gera relatório HTML com os resultados."""
        try:
            # Converte para Pandas para processamento
            pdf_pares = df_pares.toPandas()
            pdf_produtos = df_produtos.toPandas()
            
            # Gera HTML
            html_content = f"""
            <html>
            <head>
                <title>Relatório de Benchmarking</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    table {{ border-collapse: collapse; width: 100%; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; }}
                    tr:nth-child(even) {{ background-color: #f9f9f9; }}
                </style>
            </head>
            <body>
                <h1>Relatório de Benchmarking</h1>
                <h2>Pares de Produtos Similares</h2>
                {pdf_pares.to_html()}
                <h2>Produtos Analisados</h2>
                {pdf_produtos.to_html()}
            </body>
            </html>
            """
            
            # Salva HTML
            with open(f"{output_path}/relatorio.html", "w", encoding="utf-8") as f:
                f.write(html_content)
                
        except Exception as e:
            logger.error(f"Erro na geração do HTML: {str(e)}")

def main():
    """Função principal que executa o pipeline completo."""
    try:
        # Inicializa Spark
        spark = SparkSession.builder \
            .appName("Benchmarking de Preços") \
            .getOrCreate()
        
        # Inicializa componentes
        scraper = MagaluScraper(spark)
        processor = DataProcessor(spark)
        matcher = SimilarityMatcher(spark)
        reporter = ReportGenerator(spark)
        
        # Executa pipeline
        termo_busca = "smartphone"  # Exemplo
        df_produtos = scraper.extrair_produtos(termo_busca)
        df_processado = processor.processar_dados(df_produtos)
        df_pares = matcher.calcular_similaridade(df_processado)
        
        # Gera relatório
        reporter.gerar_relatorio(df_pares, df_processado)
        
        logger.info("Pipeline executado com sucesso!")
        
    except Exception as e:
        logger.error(f"Erro na execução do pipeline: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 