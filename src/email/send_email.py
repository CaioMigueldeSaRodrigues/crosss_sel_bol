import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import logging
from src.config import EMAIL_CONFIG

def send_email_with_attachment(subject, html_content, attachment_path, recipients, bcc=None):
    """
    Envia email com anexo usando configurações do Databricks
    
    Args:
        subject (str): Assunto do email
        html_content (str): Conteúdo HTML do email
        attachment_path (str): Caminho do arquivo anexo
        recipients (list): Lista de destinatários
        bcc (list): Lista de destinatários em cópia oculta
    """
    try:
        # Configura mensagem
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = EMAIL_CONFIG['sender']
        msg['To'] = ', '.join(recipients)
        
        if bcc:
            msg['Bcc'] = ', '.join(bcc)
        
        # Adiciona conteúdo HTML
        msg.attach(MIMEText(html_content, 'html'))
        
        # Adiciona anexo
        with open(attachment_path, 'rb') as f:
            attachment = MIMEApplication(f.read(), _subtype='xlsx')
            attachment.add_header('Content-Disposition', 'attachment', filename=attachment_path.split('/')[-1])
            msg.attach(attachment)
        
        # Envia email
        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender'], EMAIL_CONFIG['password'])
            server.send_message(msg)
        
        logging.info("Email enviado com sucesso")
        
    except Exception as e:
        logging.error(f"Erro ao enviar email: {str(e)}")
        raise 