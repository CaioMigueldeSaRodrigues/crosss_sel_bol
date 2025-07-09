# src/model_training.py
import mlflow
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score

def train_classification_model(df, config):
    """
    Treina um modelo de classificação RandomForest para prever se o cliente fará mais de uma compra.

    Parâmetros:
        df (pd.DataFrame): DataFrame de entrada já processado, contendo as features e a coluna alvo.
        config (dict): Dicionário de configuração com parâmetros do modelo e caminhos de dados.
    
    Retorno:
        None
    
    Efeitos colaterais:
        - Loga parâmetros e métricas no MLflow.
        - Salva o modelo treinado no MLflow.
    
    Exceções:
        KeyError: Se colunas esperadas não estiverem presentes no DataFrame.
        ValueError: Se houver problemas no split dos dados ou no treinamento.
    
    Exemplo:
        >>> train_classification_model(df_final, config)
    """
    print("--- Iniciando o treinamento do modelo ---")
    
    target_col = 'fez_mais_de_uma_compra'
    contagem_pedidos = df.groupby('customer_unique_id')['order_id'].nunique()
    df[target_col] = (df['customer_unique_id'].map(contagem_pedidos) > 1).astype(int)
    y = df[target_col]

    feature_cols = ['customer_city', 'customer_state']
    X = pd.get_dummies(df[feature_cols], drop_first=True)

    params = config['model_params']
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=params['test_size'],
        random_state=params['random_state'],
        stratify=y
    )

    with mlflow.start_run(run_name="Treino_Classificador_Cross_Sell"):
        mlflow.log_params(params)
        model = RandomForestClassifier(random_state=params['random_state'], n_jobs=-1)
        model.fit(X_train, y_train)
        
        predictions_proba = model.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, predictions_proba)
        
        mlflow.log_metric("auc", auc)
        mlflow.sklearn.log_model(model, "modelo_classificacao_cross_sell")
        
        print(f"Treinamento concluído. AUC: {auc:.4f}. Modelo registrado no MLflow.")
    return None 