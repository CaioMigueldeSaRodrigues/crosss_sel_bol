# Product Recommendation System - Databricks

Este projeto implementa um sistema de recomendação de produtos rodando no Databricks, combinando múltiplas estratégias de recomendação:
- Recomendações de cross-selling baseadas em padrões de compra
- Recomendações de produtos similares dentro da mesma categoria
- Recomendações de produtos promocionais (baseadas nos dados existentes na tabela de produtos)

## Project Structure

```
├── notebooks/                    # Databricks notebooks
│   ├── 01_data_ingestion/       # Carregamento e pré-processamento dos dados brutos
│   ├── 02_feature_engineering/  # Criação e transformação de features
│   └── 03_recommendations/      # Geração das recomendações
├── src/                         # Código-fonte
│   ├── data/                    # Módulos de processamento de dados
│   ├── models/                  # Modelos de recomendação
│   └── utils/                   # Funções utilitárias e de logging
├── sql/                         # Consultas SQL (se aplicável, para o futuro)
└── config/                      # Arquivos de configuração
```

## Features

1.  **Cross-Selling Recommendations**
    *   Analisa padrões de compra para identificar produtos frequentemente comprados juntos.
    *   Utiliza regras de associação (provenientes da tabela `hive_metastore.mawe_gold.cross_regras_varejo`) para gerar recomendações.

2.  **Similar Product Recommendations**
    *   Recomenda produtos da mesma categoria.
    *   Considera atributos do produto e pode ser expandido para preferências do cliente.

3.  **Promotional Recommendations**
    *   Recomenda produtos que estão em promoção, utilizando as informações de preço promocional (`PRECO_PROM`) disponíveis na tabela `bol.produtos_site`.

## Technical Stack

-   **Platform**: Databricks
-   **Languages**:
    *   Python
    *   PySpark
    *   SQL
-   **Data Sources**:
    *   Databricks Table: `bol.produtos_site` (informações de catálogo de produtos)
    *   Databricks Table: `bol.faturamento_centros_bol` (dados de transações e vendas)
    *   Databricks Table: `hive_metastore.mawe_gold.cross_regras_varejo` (regras de associação para cross-selling)

## Setup and Configuration

1.  **Databricks Environment**
    *   Certifique-se de ter acesso ao workspace Databricks necessário.
    *   Configure as permissões de acesso às tabelas `bol.produtos_site`, `bol.faturamento_centros_bol` e `hive_metastore.mawe_gold.cross_regras_varejo`.

2.  **Dependencies**
    *   Instale os pacotes Python necessários (ver `requirements.txt`).
    *   Configure as bibliotecas Databricks conforme a necessidade.

## Usage

1.  **Data Ingestion**
    *   Execute o notebook de ingestão de dados (`notebooks/01_data_ingestion/01_carregamento_dados.py`) para carregar e pré-processar os dados das tabelas `bol.produtos_site` e `bol.faturamento_centros_bol`.
    *   Este passo garante a consistência e relevância dos dados, aplicando filtros como estoque mínimo e transações recentes.
    *   Certifique-se de que todas as tabelas e fontes de dados necessárias no Databricks estejam acessíveis.

2.  **Recommendation Generation**
    *   Execute os notebooks subsequentes de engenharia de features e geração de recomendações para construir e aplicar os modelos.
    *   Monitore os resultados no dashboard do Databricks ou ferramenta de BI conectada.

## Contributing

1.  Fork the repository
2.  Create a feature branch
3.  Commit your changes
4.  Push to the branch
5.  Create a Pull Request

## License

This project is proprietary and Bol.

## Contact

Para dúvidas e suporte, por favor, entre em contato com a equipe de dados da Bol.

