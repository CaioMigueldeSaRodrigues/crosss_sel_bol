import logging


def setup_logger(name: str = "scraping_benchmarking") -> logging.Logger:
    """
    Configura e retorna um logger padrão para o projeto.
    O logger terá nível INFO e formato: timestamp, nível e mensagem.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger 