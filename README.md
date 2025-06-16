# Scraping Benchmarking - Databricks

Este projeto realiza o scraping de produtos do Magazine Luiza e compara com os produtos da Bemol, utilizando Databricks para processamento e armazenamento.

## Estrutura do Projeto

```
/Repos/seu_usuario/scraping_benchmarking/
├── notebooks/
│   └── 01_main_pipeline.py
├── src/
│   ├── scraping/
│   │   ├── magalu.py
│   │   └── bemol.py
│   ├── processing/
│   │   ├── cleaning.py
│   │   └── embeddings.py
│   ├── analysis/
│   │   └── similarity.py
│   ├── export/
│   │   └── export_excel.py
│   ├── email/
│   │   └── send_email.py
│   └── config.py
└── requirements.txt
```

## Configuração do Ambiente Databricks

1. **Cluster Configuration**:
   - Runtime: 13.3 LTS (includes Apache Spark 3.4.1, Scala 2.12)
   - Node Type: Standard_DS3_v2 (recomendado)
   - Min Workers: 1
   - Max Workers: 2

2. **Bibliotecas Necessárias**:
   ```
   sentence-transformers==2.2.2
   pandas==2.1.4
   openpyxl==3.1.2
   beautifulsoup4==4.12.2
   requests==2.31.0
   sendgrid==6.10.0
   ```

3. **Permissões Necessárias**:
   - Acesso à tabela `bol.feed_varejo_vtex`
   - Permissão para criar tabelas no catálogo `bol`
   - Acesso ao DBFS para armazenamento de arquivos

## Tabelas Delta

### Tabelas de Origem
- `bol.feed_varejo_vtex`: Produtos da Bemol

### Tabelas Geradas
- `bol.raw_magalu_products`: Produtos brutos do Magazine Luiza
- `bol.processed_magalu_products`: Produtos processados do Magazine Luiza
- `bol.product_matches`: Produtos correspondentes entre as lojas

## Execução

1. **Preparação**:
   ```python
   %fs mkdirs /FileStore/tables/raw
   %fs mkdirs /FileStore/tables/processed
   %fs mkdirs /FileStore/tables/exports
   %fs mkdirs /FileStore/logs
   ```

2. **Execução do Pipeline**:
   - Abra o notebook `notebooks/01_main_pipeline.py`
   - Conecte ao cluster configurado
   - Execute todas as células

## Configurações

### Email
- API Key do SendGrid configurada em `src/config.py`
- Destinatários configurados em `EMAIL_CONFIG`

### Processamento
- Threshold de similaridade: 0.7
- Modelo de embeddings: all-MiniLM-L6-v2
- Batch size: 1000

## Troubleshooting

1. **Erro de Permissão**:
   - Verificar acesso ao catálogo `bol`
   - Confirmar permissões no DBFS

2. **Erro de Memória**:
   - Aumentar número de workers
   - Ajustar configurações de memória do cluster

3. **Erro de Biblioteca**:
   - Verificar versões das bibliotecas
   - Reinstalar bibliotecas no cluster

4. **Erro de Conexão**:
   - Verificar token do Databricks
   - Confirmar configurações de rede
