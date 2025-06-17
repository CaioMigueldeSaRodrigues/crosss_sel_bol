"""
Configurações do projeto de recomendação de produtos
"""

# Configurações do Databricks
DATABRICKS_CONFIG = {
    'workspace_url': 'https://your-workspace.cloud.databricks.com',
    'cluster_id': 'your-cluster-id'
}

# Configurações do OneDrive
ONEDRIVE_CONFIG = {
    'client_id': 'your-client-id',
    'client_secret': 'your-client-secret',
    'tenant_id': 'your-tenant-id'
}

# Configurações de Recomendação
RECOMMENDATION_CONFIG = {
    'min_support': 0.01,  # Suporte mínimo para regras de associação
    'min_confidence': 0.5,  # Confiança mínima para regras de associação
    'max_recommendations': 5,  # Número máximo de recomendações por produto
    'similarity_threshold': 0.7  # Limiar de similaridade para produtos similares
}

# Configurações de Promoções
PROMOTION_CONFIG = {
    'excel_path': 'Documentos/SuperOfertas_CrossSell_NOVO_2025_BOL.xlsx',
    'sheet_name': 'COMERCIAL_(AM_RR)',
    'update_frequency': 'daily'  # Frequência de atualização das promoções
}

# Configurações de Logging
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': 'logs/recommendation_system.log'
}

# Configurações de Cache
CACHE_CONFIG = {
    'enabled': True,
    'ttl': 3600  # Tempo de vida do cache em segundos
} 