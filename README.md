# Product Recommendation System - Databricks

This project implements a product recommendation system running on Databricks, combining multiple recommendation strategies:
- Cross-selling recommendations based on purchase patterns
- Similar product recommendations within the same category
- Promotional product recommendations

## Project Structure

```
├── notebooks/                    # Databricks notebooks
│   ├── 01_data_ingestion/       # Data loading and preprocessing
│   ├── 02_feature_engineering/  # Feature creation and transformation
│   └── 03_recommendations/      # Recommendation generation
├── src/                         # Source code
│   ├── data/                    # Data processing modules
│   ├── models/                  # Recommendation models
│   └── utils/                   # Utility functions
├── sql/                         # SQL queries
│   ├── cross_selling/          # Cross-selling related queries
│   └── promotions/             # Promotional product queries
└── config/                      # Configuration files
```

## Features

1. **Cross-Selling Recommendations**
   - Analyzes purchase patterns to identify products frequently bought together
   - Uses association rules mining to generate recommendations

2. **Similar Product Recommendations**
   - Recommends products from the same category
   - Considers product attributes and customer preferences

3. **Promotional Recommendations**
   - Integrates with promotional data from Excel sheets
   - Recommends products currently on promotion

## Technical Stack

- **Platform**: Databricks
- **Languages**: 
  - Python
  - PySpark
  - SQL
- **Data Sources**:
  - Databricks Tables
  - Excel Files (Promotions)
  - OneDrive Integration

## Setup and Configuration

1. **Databricks Environment**
   - Ensure you have access to the required Databricks workspace
   - Configure necessary permissions for OneDrive access

2. **Dependencies**
   - Install required Python packages
   - Configure Databricks libraries

3. **Data Access**
   - Set up connections to required data sources
   - Configure authentication for OneDrive access

## Usage

1. **Data Ingestion**
   - Execute os notebooks de ingestão de dados para carregar e pré-processar os dados.
   - **Atualização:** Foram feitos ajustes na forma como os dados de produtos e transações são carregados, incluindo mapeamento de colunas e filtros para garantir a consistência e relevância dos dados (ex: produtos com estoque > 4, transações dos últimos 90 dias).
   - **Novo:** Inclusão do carregamento de dados promocionais do OneDrive, utilizando o `DataProcessor` para padronizar e mesclar as informações.
   - Certifique-se de que todas as tabelas e fontes de dados necessárias estejam acessíveis.

2. **Recommendation Generation**
   - Execute the recommendation notebooks
   - Monitor the results in the Databricks dashboard

3. **Promotional Updates**
   - Update promotional data in the Excel sheet
   - Run the promotional recommendation process

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is proprietary and confidential.

## Contact

For questions and support, please contact the development team.

Este é um teste de atualização do README.
