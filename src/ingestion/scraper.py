import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.logger_config import setup_logger
from src import config

logger = setup_logger(__name__)

def get_http_session() -> requests.Session:
    """Configura e retorna uma sessão HTTP com retries."""
    session = requests.Session()
    session.headers.update({"User-Agent": config.DEFAULT_USER_AGENT})
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def fetch_html(session: requests.Session, url: str) -> str | None:
    """Busca o conteúdo HTML de uma URL com tratamento de erro."""
    try:
        response = session.get(url, timeout=config.REQUESTS_TIMEOUT_SECONDS)
        response.raise_for_status()  # Lança exceção para status 4xx/5xx
        return response.text
    except requests.RequestException as e:
        logger.error(f"Falha ao buscar URL {url}: {e}")
        return None 