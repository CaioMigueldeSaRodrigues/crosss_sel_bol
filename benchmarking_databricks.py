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
        """Clean and standardize price values."""
        try:
            if pd.isna(price):
                return 0.0
                
            price_str = str(price)
            
            # Remove currency symbols and text
            price_str = re.sub(r'[^\d.,]', '', price_str)
            
            # Handle different price formats
            if ',' in price_str and '.' in price_str:
                # Format: 1.234,56 or 1,234.56
                if price_str.rindex(',') > price_str.rindex('.'):
                    # Format: 1.234,56
                    price_str = price_str.replace('.', '').replace(',', '.')
                else:
                    # Format: 1,234.56
                    price_str = price_str.replace(',', '')
            else:
                # Format: 1234,56 or 1234.56
                price_str = price_str.replace(',', '.')
            
            return float(price_str)
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
        elif score >= 0.75:
            return "similar"
        elif score >= 0.5:
            return "moderadamente similar"
        else:
            return "pouco similar"

# COMMAND ----------

class BenchmarkingProcessor:
    def __init__(self, similarity_threshold: float = 0.75):
        self.similarity_processor = SimilarityProcessor(similarity_threshold)
        self.price_processor = PriceProcessor()
        self.text_processor = TextProcessor()

    def process_dataframes(self, df_magalu: pd.DataFrame, df_bemol: pd.DataFrame) -> pd.DataFrame:
        """Process and match products between marketplaces."""
        try:
            # Prepare dataframes
            df_magalu["marketplace"] = "Magalu"
            df_bemol["marketplace"] = "Bemol"

            # Clean prices
            df_magalu["price"] = df_magalu["price"].apply(self.price_processor.clean_price)
            df_bemol["price"] = df_bemol["price"].apply(self.price_processor.clean_price)

            # Process embeddings
            df_magalu["embedding"] = df_magalu["embedding"].apply(lambda x: np.array(x) if pd.notnull(x) else None)
            df_bemol["embedding"] = df_bemol["embedding"].apply(lambda x: np.array(x) if pd.notnull(x) else None)

            # Remove null embeddings
            df_magalu = df_magalu[df_magalu["embedding"].notnull()]
            df_bemol = df_bemol[df_bemol["embedding"].notnull()]

            # Process titles
            df_magalu["processed_title"] = df_magalu["title"].apply(self.text_processor.preprocess_text)
            df_bemol["processed_title"] = df_bemol["title"].apply(self.text_processor.preprocess_text)

            return self._match_products(df_magalu, df_bemol)

        except Exception as e:
            logger.error(f"Error processing dataframes: {e}")
            raise

    def _match_products(self, df_magalu: pd.DataFrame, df_bemol: pd.DataFrame) -> pd.DataFrame:
        """Match products between marketplaces using embeddings."""
        result = []
        pares_usados_bemol = set()

        for idx, magalu_row in df_magalu.iterrows():
            magalu_embed = magalu_row["embedding"]
            scores = cosine_similarity([magalu_embed], df_bemol["embedding"].tolist())[0]
            best_idx = np.argmax(scores)
            best_score = scores[best_idx]

            if best_score >= self.similarity_processor.threshold and best_idx not in pares_usados_bemol:
                bemol_row = df_bemol.iloc[best_idx]
                pares_usados_bemol.add(best_idx)

                # Process URLs
                magalu_url = self._process_url(magalu_row.url, "magazineluiza.com.br")
                bemol_url = self._process_url(bemol_row.url, "bemol.com.br")

                # Add matched pairs
                result.extend([
                    self._create_product_dict(magalu_row, magalu_url, best_score),
                    self._create_product_dict(bemol_row, bemol_url, best_score)
                ])

        # Add exclusive products
        result.extend(self._add_exclusive_products(df_magalu, df_bemol, result))

        # Create final dataframe
        df_final = pd.DataFrame(result)
        df_final = self._post_process_results(df_final)

        return df_final

    def _process_url(self, url: str, domain: str) -> str:
        """Process and standardize URLs."""
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

    def _add_exclusive_products(self, df_magalu: pd.DataFrame, df_bemol: pd.DataFrame, 
                              result: List[Dict]) -> List[Dict]:
        """Add exclusive products from both marketplaces."""
        exclusive_products = []
        
        # Get matched titles
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

# COMMAND ----------

# Initialize processor
processor = BenchmarkingProcessor(similarity_threshold=0.75)

# Read data from Databricks tables
df_magalu = spark.table("silver.embeddings_magalu_completo").toPandas()
df_bemol = spark.table("silver.embeddings_bemol").toPandas()

# Process data
df_final = processor.process_dataframes(df_magalu, df_bemol)

# Save results to Databricks
spark.createDataFrame(df_final).createOrReplaceTempView("tempview_benchmarking_pares")

# Export to Excel in Databricks File System
excel_path = "/dbfs/FileStore/tempview_benchmarking_produtos.xlsx"
df_final.to_excel(excel_path, index=False)

logger.info(f"✅ Exportação final concluída com sucesso: {excel_path}")

# COMMAND ----------

# Display results
display(df_final) 