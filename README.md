# Product Recommendation System

Este projeto implementa um sistema de recomendação de produtos, com foco em cross-selling, produtos similares e promoções.

## Estrutura do Projeto

```
├── run_pipeline.py               # Orquestrador principal do pipeline de dados e modelo
├── src/                         # Código-fonte (processamento, modelos, utilitários)
│   ├── data_processing.py
│   ├── model_training.py
│   └── ...
├── config.yaml                   # Configuração de caminhos e parâmetros
├── requirements.txt              # Dependências Python
└── data/                        # Dados de entrada (definidos em config.yaml)
```

## Como Executar

1. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure os caminhos dos dados em `config.yaml`:**
   - Exemplo:
     ```yaml
     data_paths:
       customers: data/customers.csv
       orders: data/orders.csv
     model_params:
       test_size: 0.2
       random_state: 42
     ```

3. **Execute o pipeline:**
   ```bash
   python run_pipeline.py
   ```

   O script irá:
   - Carregar configurações e dados.
   - Realizar pré-processamento.
   - Treinar o modelo de classificação.
   - Logar resultados no MLflow.

## Principais Funcionalidades

- **Cross-Selling:** Identifica clientes com potencial para novas compras.
- **Pré-processamento robusto:** Limpeza e preparação dos dados.
- **Treinamento automatizado:** Pipeline de machine learning integrado ao MLflow.

## Contribuição

1. Fork o repositório
2. Crie uma branch de feature
3. Commit suas alterações
4. Push para a branch
5. Abra um Pull Request

## Licença

Este projeto é proprietário e confidencial.

## Contato

Para dúvidas e suporte, por favor, entre em contato com a equipe de dados da Bol.

