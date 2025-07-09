# run_pipeline.py - Orquestrador Principal do Projeto

import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

import yaml
import pandas as pd
import sys
from src.data_processing import preprocessar_dados_cliente
from src.model_training import train_classification_model

def main():
    """
    Função principal para orquestrar a execução do pipeline.
    """
    print("=================================================")
    print("==    INICIANDO EXECUÇÃO DO PIPELINE DE DADOS   ==")
    print("=================================================")

    # --- ETAPA 1: CARREGAR CONFIGURAÇÃO E DADOS ---
    print("\n[ETAPA 1/3] Carregando configuração e dados...")
    try:
        with open('config.yaml', 'r') as file:
            config = yaml.safe_load(file)
        
        # Carrega os dataframes usando os caminhos do config
        df_customers = pd.read_csv(config['data_paths']['customers'])
        df_orders = pd.read_csv(config['data_paths']['orders'])
        print("[SUCCESS] Configuração e dados carregados.")

    except FileNotFoundError as e:
        print(f"[ERROR] Arquivo não encontrado: {e}.")
        print("[FAIL] Pipeline encerrado. Verifique o config.yaml e a pasta /data.")
        sys.exit(1) # Encerra o script com código de erro

    # --- ETAPA 2: PRÉ-PROCESSAMENTO ---
    print("\n[ETAPA 2/3] Iniciando pré-processamento de dados...")
    df_final = preprocessar_dados_cliente(df_customers, df_orders)
    print("[SUCCESS] Pré-processamento concluído.")

    # --- ETAPA 3: TREINAMENTO DO MODELO ---
    print("\n[ETAPA 3/3] Iniciando treinamento do modelo...")
    train_classification_model(df_final, config)
    print("[SUCCESS] Treinamento concluído.")

    print("\n=================================================")
    print("==    PIPELINE FINALIZADO COM SUCESSO          ==")
    print("=================================================")


if __name__ == "__main__":
    main() 