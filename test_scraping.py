import sys
import logging
from src.scraping.magalu import scrape_magalu
from src.scraping.bemol import scrape_bemol
from src.email.send_email import send_email
from src.config import EMAIL_CONFIG

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_scraping_and_email():
    try:
        # Testar scraping do Magazine Luiza
        logger.info("Iniciando scraping do Magazine Luiza...")
        magalu_df = scrape_magalu()
        logger.info(f"Scraping do Magazine Luiza concluído. {len(magalu_df)} produtos coletados.")
        
        # Testar scraping da Bemol
        logger.info("Iniciando scraping da Bemol...")
        bemol_df = scrape_bemol()
        logger.info(f"Scraping da Bemol concluído. {len(bemol_df)} produtos coletados.")
        
        # Preparar dados para o email
        magalu_sample = magalu_df.head(5).to_html()
        bemol_sample = bemol_df.head(5).to_html()
        
        # Enviar email de teste
        subject = "Teste de Scraping e Email"
        html_content = f"""
        <h2>Teste de Scraping e Email</h2>
        
        <h3>Produtos Magazine Luiza (Amostra):</h3>
        {magalu_sample}
        
        <h3>Produtos Bemol (Amostra):</h3>
        {bemol_sample}
        
        <p>Total de produtos Magazine Luiza: {len(magalu_df)}</p>
        <p>Total de produtos Bemol: {len(bemol_df)}</p>
        """
        
        logger.info("Enviando email de teste...")
        send_email(subject, html_content)
        logger.info("Email enviado com sucesso!")
        
        return True
        
    except Exception as e:
        logger.error(f"Erro durante o teste: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_scraping_and_email()
    sys.exit(0 if success else 1) 