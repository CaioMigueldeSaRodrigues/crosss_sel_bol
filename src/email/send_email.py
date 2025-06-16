from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content
import pandas as pd
from ..config import EMAIL_CONFIG

def send_email(df_matches):
    """
    Envia um email com os resultados do matching de produtos.
    
    Args:
        df_matches (pd.DataFrame): DataFrame com produtos correspondentes
        
    Returns:
        bool: True se o email foi enviado com sucesso, False caso contrário
    """
    try:
        if not EMAIL_CONFIG['enabled']:
            print("Envio de email desabilitado nas configurações")
            return False
            
        # Cria cliente SendGrid
        sg = SendGridAPIClient(EMAIL_CONFIG['api_key'])
        
        # Prepara conteúdo do email
        html_content = f"""
        <h2>Relatório de Produtos Correspondentes</h2>
        <p>Total de produtos correspondentes: {len(df_matches)}</p>
        
        <table border="1" style="border-collapse: collapse; width: 100%;">
            <tr>
                <th>Produto Bemol</th>
                <th>Preço Bemol</th>
                <th>Produto Magazine Luiza</th>
                <th>Preço Magazine Luiza</th>
                <th>Score de Similaridade</th>
            </tr>
        """
        
        # Adiciona linhas da tabela
        for _, row in df_matches.iterrows():
            html_content += f"""
            <tr>
                <td>{row['bemol_title']}</td>
                <td>R$ {row['bemol_price']:.2f}</td>
                <td>{row['magalu_title']}</td>
                <td>R$ {row['magalu_price']:.2f}</td>
                <td>{row['similarity_score']:.2f}</td>
            </tr>
            """
            
        html_content += "</table>"
        
        # Cria mensagem
        message = Mail(
            from_email=Email(EMAIL_CONFIG['from_email']),
            to_emails=[To(email) for email in EMAIL_CONFIG['to_emails']],
            subject=EMAIL_CONFIG['subject'],
            html_content=Content("text/html", html_content)
        )
        
        # Envia email
        response = sg.send(message)
        
        print(f"Email enviado com sucesso. Status code: {response.status_code}")
        return True
        
    except Exception as e:
        print(f"Erro ao enviar email: {str(e)}")
        return False 