from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content
import pandas as pd
from ..config import EMAIL_CONFIG

def send_email(subject, html_content):
    """
    Envia um email com os resultados do matching de produtos.
    
    Args:
        subject (str): Assunto do email
        html_content (str): Conteúdo HTML do email
        
    Returns:
        bool: True se o email foi enviado com sucesso, False caso contrário
    """
    try:
        if not EMAIL_CONFIG['enabled']:
            print("Envio de email desabilitado nas configurações")
            return False
            
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
            print("Email enviado com sucesso!")
            return True
        else:
            print(f"Erro ao enviar email. Status code: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"Erro ao enviar email: {str(e)}")
        return False 