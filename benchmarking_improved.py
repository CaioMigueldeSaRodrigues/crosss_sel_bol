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
            # Remove caracteres especiais invisíveis e palavras como "ou", "R$", etc
            preco_str = preco_str.replace('\xa0', ' ').replace('R$', '').replace('ou', '')
            preco_str = preco_str.strip()

            # Primeira tentativa: Regex específico para formato brasileiro (mais preciso)
            match = re.search(r'\d{1,3}(?:\.\d{3})*,\d{2}', preco_str)
            if match:
                preco_str = match.group(0).replace('.', '').replace(',', '.')
                return float(preco_str)

            # Segunda tentativa: Regex mais genérico para outros formatos
            match = re.search(r'(\d{1,3}(?:\.\d{3})*,\d{2})', preco_str)
            if match:
                preco_str = match.group(1).replace('.', '').replace(',', '.')
                return float(preco_str)

            # Fallback: substituição bruta (menos confiável)
            preco_str = preco_str.replace('.', '').replace(',', '.')
            return float(preco_str)
        except Exception as e:
            logger.warning(f"Error cleaning price {price}: {e}")
            return 0.0

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
        elif score >= 0.95:
            return "alta"
        elif score >= 0.85:
            return "moderada"
        else:
            return "baixa"

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

                # Classify similarity level
                nivel = self.similarity_processor.classify_similarity(best_score)

                # Add matched pairs with similarity level
                result.extend([
                    self._create_product_dict(magalu_row, magalu_url, best_score, nivel),
                    self._create_product_dict(bemol_row, bemol_url, best_score, nivel)
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

    def _create_product_dict(self, row: pd.Series, url: str, similarity: float, nivel: str) -> Dict:
        """Create product dictionary with standardized format."""
        return {
            "title": row.title,
            "marketplace": row.marketplace,
            "price": row.price,
            "url": url,
            "exclusividade": "não",
            "similaridade": similarity,
            "nivel_similaridade": nivel
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
            exclusive_products.append(self._create_product_dict(row, url, -1, "baixa"))
        
        # Add exclusive Bemol products
        for row in df_bemol[~df_bemol["title"].isin(matched_titles)].itertuples():
            url = self._process_url(row.url, "bemol.com.br")
            exclusive_products.append(self._create_product_dict(row, url, -1, "baixa"))
        
        return exclusive_products

    def _post_process_results(self, df_final: pd.DataFrame) -> pd.DataFrame:
        """Post-process the final results."""
        # Redefine o índice e garante paridade
        df_final = df_final.reset_index(drop=True)
        if len(df_final) % 2 != 0:
            df_final = df_final.iloc[:-1]

        # Sort results
        df_final = df_final.sort_values(
            by=["exclusividade", "similaridade"], 
            ascending=[True, False]
        ).reset_index(drop=True)

        # Calculate price differences only for high similarity pairs
        df_final["diferenca_percentual"] = None
        for i in range(0, len(df_final), 2):
            sim = df_final.loc[i, "similaridade"]
            if sim >= 0.90:  # Aplica cálculo apenas para pares com alta similaridade
                p1 = float(df_final.loc[i, "price"])
                p2 = float(df_final.loc[i+1, "price"])
                media = (p1 + p2) / 2
                dif_percentual = abs(p1 - p2) / media * 100
                df_final.loc[i:i+1, "diferenca_percentual"] = dif_percentual

        # Format percentage values
        df_final["diferenca_percentual"] = df_final["diferenca_percentual"].apply(
            lambda x: f"{x:.2f}%" if pd.notnull(x) else None
        )

        return df_final

def main():
    try:
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

    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__":
    main() 