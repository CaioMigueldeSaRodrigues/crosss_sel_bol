"""
Configurações do projeto
"""

# Configurações do Spark
SPARK_CONFIG = {
    'app_name': 'Ingestão de Dados',
    'shuffle_partitions': 200,
    'off_heap_size': '10g',
    'adaptive_enabled': True,
    'coalesce_enabled': True
}

# Configurações de Dados
DATA_CONFIG = {
    'produtos': {
        'table': 'bol.produtos_site',
        'min_stock': 4,
        'centro': 102
    },
    'transacoes': {
        'table': 'bol.faturamento_centros_bol',
        'days_lookback': 90
    },
    'regras_associacao': {
        'table': 'hive_metastore.mawe_gold.cross_regras_varejo',
        'data_extraction_field': 'DATA_EXTRACAO'
    }
}

# Configurações de Validação
VALIDATION_CONFIG = {
    'produtos': {
        'required_columns': ['id', 'nome', 'categoria', 'preco', 'promocao', 'quantidade_estoque'],
        'numeric_columns': ['preco', 'promocao', 'quantidade_estoque'],
        'min_price': 0,
        'min_stock': 0
    },
    'transacoes': {
        'required_columns': ['pedido_id', 'produto_id', 'quantidade', 'valor_total', 'DT_FATURAMENTO', 'cliente_id'],
        'numeric_columns': ['quantidade', 'valor_total'],
        'min_quantity': 0,
        'min_value': 0
    }
}

# Configurações de Logging
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'date_format': '%Y-%m-%d %H:%M:%S'
}

# Configurações de Performance
PERFORMANCE_CONFIG = {
    'cache_enabled': True,
    'job_group': 'processamento_dados',
    'job_description': 'Processamento de dados para recomendação'
}

# Configurações de Armazenamento
STORAGE_CONFIG = {
    'format': 'delta',
    'mode': 'overwrite',
    'merge_schema': True,
    'overwrite_schema': True
} 