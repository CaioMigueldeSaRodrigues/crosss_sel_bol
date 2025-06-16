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
   - Run the data ingestion notebooks to load and preprocess data
   - Ensure all required tables are available

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
