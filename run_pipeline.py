# run_pipeline.py - Orquestrador Principal do Projeto

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import yaml
import pandas as pd
import mlflow
from src.data_processing import preprocessar_dados_cliente
from src.model_training import train_classification_model

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def configurar_mlflow():
    """
    Configura o MLflow para uso local se não estiver em ambiente Databricks.
    """
    try:
        # Tenta detectar ambiente Databricks
        import databricks_cli  # noqa: F401
        print("Usando MLflow no Databricks.")
    except ImportError:
        # Fallback: usa tracking local
        mlflow.set_tracking_uri("file://" + os.path.join(BASE_DIR, "mlruns"))
        print("MLflow configurado para uso local em ./mlruns")

def carregar_config_e_dados(config_path):
    """
    Carrega o arquivo de configuração e os dados de clientes e pedidos.
    Retorna: (config, df_customers, df_orders)
    """
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        customers_path = os.path.join(BASE_DIR, config['data_paths']['customers'])
        orders_path = os.path.join(BASE_DIR, config['data_paths']['orders'])
        df_customers = pd.read_csv(customers_path)
        df_orders = pd.read_csv(orders_path)
        print("[SUCCESS] Configuração e dados carregados.")
        return config, df_customers, df_orders
    except FileNotFoundError as e:
        print(f"[ERROR] Arquivo não encontrado: {e}.")
        print("[FAIL] Pipeline encerrado. Verifique o config.yaml e a pasta /data.")
        sys.exit(1)

def executar_preprocessamento(df_customers, df_orders):
    """
    Executa o pré-processamento dos dados e retorna o DataFrame final.
    """
    print("\n[ETAPA 2/3] Iniciando pré-processamento de dados...")
    df_final = preprocessar_dados_cliente(df_customers, df_orders)
    print("[SUCCESS] Pré-processamento concluído.")
    return df_final

def treinar_modelo(df_final, config):
    """
    Treina o modelo de classificação usando os dados processados e a configuração.
    """
    print("\n[ETAPA 3/3] Iniciando treinamento do modelo...")
    train_classification_model(df_final, config)
    print("[SUCCESS] Treinamento concluído.")

def main():
    print("=================================================")
    print("==    INICIANDO EXECUÇÃO DO PIPELINE DE DADOS   ==")
    print("=================================================")

    configurar_mlflow()

    # --- ETAPA 1: CARREGAR CONFIGURAÇÃO E DADOS ---
    print("\n[ETAPA 1/3] Carregando configuração e dados...")
    config_path = os.path.join(BASE_DIR, 'config.yaml')
    config, df_customers, df_orders = carregar_config_e_dados(config_path)

    # --- ETAPA 2: PRÉ-PROCESSAMENTO ---
    df_final = executar_preprocessamento(df_customers, df_orders)

    # --- ETAPA 3: TREINAMENTO DO MODELO ---
    treinar_modelo(df_final, config)

    print("\n=================================================")
    print("==    PIPELINE FINALIZADO COM SUCESSO          ==")
    print("=================================================")

if __name__ == "__main__":
    main() 