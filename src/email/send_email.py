from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content
import pandas as pd
import logging
from src.config import EMAIL_CONFIG, LOGGING_CONFIG

# Configuração de logging
logging.basicConfig(
    level=getattr(logging, LOGGING_CONFIG["level"]),
    format=LOGGING_CONFIG["format"]
)
logger = logging.getLogger(__name__)

def send_email(subject, html_content):
    """
    Envia um email com os resultados do matching de produtos.
    
    Args:
        subject (str): Assunto do email
        html_content (str): Conteúdo HTML do email
        
    Returns:
        bool: True se o email foi enviado com sucesso, False caso contrário
        
    Raises:
        ValueError: Se o assunto ou conteúdo forem vazios
        ValueError: Se as configurações de email estiverem inválidas
    """
    if not subject or not subject.strip():
        raise ValueError("Assunto do email não pode ser vazio")
        
    if not html_content or not html_content.strip():
        raise ValueError("Conteúdo do email não pode ser vazio")
        
    if not EMAIL_CONFIG['enabled']:
        logger.info("Envio de email desabilitado nas configurações")
        return False
        
    if not EMAIL_CONFIG['api_key']:
        raise ValueError("API key do SendGrid não configurada")
        
    if not EMAIL_CONFIG['from_email']:
        raise ValueError("Email de origem não configurado")
        
    if not EMAIL_CONFIG['to_emails']:
        raise ValueError("Lista de destinatários vazia")
        
    try:
        # Cria cliente SendGrid
        sg = SendGridAPIClient(EMAIL_CONFIG['api_key'])
        
        # Prepara o email
        message = Mail(
            from_email=Email(EMAIL_CONFIG['from_email']),
            to_emails=[To(email) for email in EMAIL_CONFIG['to_emails']],
            subject=subject,
            html_content=Content("text/html", html_content)
        )
        
        # Envia o email
        response = sg.send(message)
        
        if response.status_code == 202:
            logger.info("Email enviado com sucesso!")
            return True
        else:
            logger.error(f"Erro ao enviar email. Status code: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Erro ao enviar email: {str(e)}")
        return False 