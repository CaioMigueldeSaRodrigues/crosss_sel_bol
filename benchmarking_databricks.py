# Databricks notebook source
# COMMAND ----------

# Install required packages
!pip install unidecode nltk

# COMMAND ----------

import numpy as np
import pandas as pd
import re
from sklearn.metrics.pairwise import cosine_similarity
from typing import List, Dict, Union, Tuple
import logging
from unidecode import unidecode
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import warnings
warnings.filterwarnings('ignore')

# Configure logging for Databricks
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize NLTK data in Databricks
try:
    nltk.data.path.append("/dbfs/FileStore/nltk_data")
    nltk.download('punkt', download_dir="/dbfs/FileStore/nltk_data", quiet=True)
    nltk.download('stopwords', download_dir="/dbfs/FileStore/nltk_data", quiet=True)
except Exception as e:
    logger.warning(f"Could not download NLTK data: {e}")

# COMMAND ----------

class TextProcessor:
    def __init__(self):
        try:
            self.stop_words = set(stopwords.words('portuguese'))
        except Exception as e:
            logger.warning(f"Could not load stopwords: {e}")
            self.stop_words = set()

    def preprocess_text(self, text: str) -> str:
        """Clean and normalize text for better comparison."""
        if not isinstance(text, str):
            return ""
        
        # Convert to lowercase and remove accents
        text = unidecode(text.lower())
        
        # Remove special characters and extra spaces
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Remove stopwords
        tokens = word_tokenize(text)
        tokens = [t for t in tokens if t not in self.stop_words]
        
        return ' '.join(tokens)

# COMMAND ----------

class PriceProcessor:
    @staticmethod
    def clean_price(price: Union[str, float, int]) -> float:
        """Clean and standardize price values.
        
        Handles multiple price formats:
        - Brazilian format: 5.886,00
        - Mixed formats: R$ 5.886,00 ou 5.886,00
        - Simple formats: 5886,00
        """
        try:
            if pd.isna(price):
                return 0.0
                
            preco_str = str(price)
            # Primeira tentativa: Regex específico para formato brasileiro
            match = re.search(r'(\d{1,3}(?:\.\d{3})*,\d{2})', preco_str)
            if match:
                preco_str = match.group(1).replace('.', '').replace(',', '.')
                return float(preco_str)

            # Segunda tentativa: Limpeza básica
            preco_str = preco_str.replace('R$', '').replace('ou', '').strip()
            preco_str = preco_str.replace('.', '').replace(',', '.')
            return float(preco_str)
        except Exception as e:
            logger.warning(f"Error cleaning price {price}: {e}")
            return 0.0

# COMMAND ----------

class SimilarityProcessor:
    def __init__(self, threshold: float = 0.75):
        self.threshold = threshold
        self.text_processor = TextProcessor()

    def calculate_similarity(self, embed1: np.ndarray, embed2: np.ndarray) -> float:
        """Calculate cosine similarity between two embeddings."""
        try:
            return cosine_similarity([embed1], [embed2])[0][0]
        except Exception as e:
            logger.error(f"Error calculating similarity: {e}")
            return 0.0

    def classify_similarity(self, score: float) -> str:
        """Classify similarity score into categories."""
        if score == -1:
            return "exclusivo"
        elif score >= 0.85:
            return "muito similar"
        elif score >= 0.5:
            return "moderadamente similar"
        else:
            return "pouco similar"

# COMMAND ----------

class BenchmarkingProcessor:
    def __init__(self, text_processor: TextProcessor, similarity_processor: SimilarityProcessor, price_processor: PriceProcessor):
        self.text_processor = text_processor
        self.similarity_processor = similarity_processor
        self.price_processor = price_processor

    def process_data(self, df_magalu: pd.DataFrame, df_bemol: pd.DataFrame) -> pd.DataFrame:
        """Process data from both marketplaces."""
        # Prepare dataframes
        df_magalu["marketplace"] = "Magalu"
        df_bemol["marketplace"] = "Bemol"

        # Clean prices
        df_magalu["price"] = df_magalu["price"].apply(self.price_processor.clean_price)
        df_bemol["price"] = df_bemol["price"].apply(self.price_processor.clean_price)

        # Convert embeddings
        df_magalu["embedding"] = df_magalu["embedding"].apply(np.array)
        df_bemol["embedding"] = df_bemol["embedding"].apply(np.array)

        # Remove null embeddings
        df_magalu = df_magalu[df_magalu["embedding"].notnull()]
        df_bemol = df_bemol[df_bemol["embedding"].notnull()]

        # Calculate similarity matrix
        sim_matrix = cosine_similarity(df_magalu["embedding"].tolist(), df_bemol["embedding"].tolist())
        matched_indices = sim_matrix.argmax(axis=1)
        matched_scores = sim_matrix.max(axis=1)

        # Match products
        result = []
        for idx, (magalu_row, match_idx, score) in enumerate(zip(df_magalu.itertuples(), matched_indices, matched_scores)):
            if score >= self.similarity_processor.threshold:
                bemol_row = df_bemol.iloc[match_idx]
                
                # Process URLs
                magalu_url = self._process_url(magalu_row.url, "magazineluiza.com.br")
                bemol_url = self._process_url(bemol_row.url, "bemol.com.br")

                # Add matched pairs
                result.extend([
                    self._create_product_dict(magalu_row, magalu_url, score),
                    self._create_product_dict(bemol_row, bemol_url, score)
                ])

        # Add exclusive products
        result.extend(self._add_exclusive_products(df_magalu, df_bemol, result))

        # Create final dataframe
        df_final = pd.DataFrame(result)
        df_final = self._post_process_results(df_final)

        return df_final

    def _process_url(self, url: str, domain: str) -> str:
        """Process URL to ensure correct format."""
        if not str(url).startswith("http"):
            return f"https://www.{domain}{url}"
        return url

    def _create_product_dict(self, row: pd.Series, url: str, similarity: float) -> Dict:
        """Create product dictionary with standardized format."""
        return {
            "title": row.title,
            "marketplace": row.marketplace,
            "price": row.price,
            "url": url,
            "exclusividade": "não",
            "similaridade": similarity
        }

    def _add_exclusive_products(self, df_magalu: pd.DataFrame, df_bemol: pd.DataFrame, result: List[Dict]) -> List[Dict]:
        """Add exclusive products from both marketplaces."""
        exclusive_products = []
        matched_titles = {r["title"] for r in result}

        # Add exclusive Magalu products
        for row in df_magalu[~df_magalu["title"].isin(matched_titles)].itertuples():
            url = self._process_url(row.url, "magazineluiza.com.br")
            exclusive_products.append(self._create_product_dict(row, url, -1))
        
        # Add exclusive Bemol products
        for row in df_bemol[~df_bemol["title"].isin(matched_titles)].itertuples():
            url = self._process_url(row.url, "bemol.com.br")
            exclusive_products.append(self._create_product_dict(row, url, -1))
        
        return exclusive_products

    def _post_process_results(self, df_final: pd.DataFrame) -> pd.DataFrame:
        """Post-process the final results."""
        # Sort results
        df_final = df_final.sort_values(
            by=["exclusividade", "similaridade"], 
            ascending=[True, False]
        ).reset_index(drop=True)

        # Add similarity classification
        df_final["nivel_similaridade"] = df_final["similaridade"].apply(
            self.similarity_processor.classify_similarity
        )

        # Calculate price differences
        df_final["diferenca_percentual"] = df_final.groupby(
            df_final.index // 2
        )["price"].transform(
            lambda x: self._calculate_price_difference(x.iloc[0], x.iloc[1]) 
            if len(x) == 2 else None
        )

        return df_final

    @staticmethod
    def _calculate_price_difference(p1: float, p2: float) -> float:
        """Calculate percentage difference between prices."""
        try:
            return abs(p1 - p2) / ((p1 + p2) / 2) * 100
        except:
            return None

def main():
    try:
        # Initialize processors
        text_processor = TextProcessor()
        similarity_processor = SimilarityProcessor()
        price_processor = PriceProcessor()
        benchmarking_processor = BenchmarkingProcessor(
            text_processor, 
            similarity_processor, 
            price_processor
        )

        # Read data from Spark tables
        df_magalu = spark.table("silver.embeddings_magalu_completo").toPandas()
        df_bemol = spark.table("silver.embeddings_bemol").toPandas()

        # Process data
        df_final = benchmarking_processor.process_data(df_magalu, df_bemol)

        # Save results as TempView for SQL queries
        spark.createDataFrame(df_final).createOrReplaceTempView("tempview_benchmarking_pares")

        # Save final results to DBFS
        output_path = "/dbfs/mnt/datalake/silver/benchmarking/benchmarking_results.parquet"
        df_final.to_parquet(output_path, index=False)
        logger.info(f"Results saved to {output_path}")

    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise

if __name__ == "__main__":
    main()

# COMMAND ----------

# Display results
display(df_final) 