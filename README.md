# Scraping Benchmarking - Databricks

Este projeto realiza o scraping de produtos do Magazine Luiza e compara com os produtos da Bemol, utilizando Databricks para processamento e armazenamento.

## Estrutura do Projeto

```
/Repos/seu_usuario/scraping_benchmarking/
├── notebooks/
│   ├── 00_setup_cluster.py    # Notebook de configuração do ambiente
│   └── 01_main_pipeline.py    # Pipeline principal
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

## Iniciando o Projeto

### 1. Configuração do Cluster

1. Crie um novo cluster no Databricks:
   - Runtime: 13.3 LTS (includes Apache Spark 3.4.1, Scala 2.12)
   - Node Type: Standard_DS3_v2 (recomendado)
   - Min Workers: 1
   - Max Workers: 2

2. Adicione as seguintes bibliotecas ao cluster:
   ```
   sentence-transformers==2.2.2
   pandas==2.1.4
   openpyxl==3.1.2
   beautifulsoup4==4.12.2
   requests==2.31.0
   sendgrid==6.10.0
   delta-spark==3.0.0
   ```

### 2. Configuração do Repositório

1. Clone o repositório no Databricks:
   - Vá para "Repos" no menu lateral
   - Clique em "Add Repo"
   - Cole a URL: `https://github.com/CaioMigueldeSaRodrigues/scraping_benchmarking.git`
   - Dê um nome para o repositório (ex: "scraping_benchmarking")

### 3. Execução do Pipeline

1. Execute o notebook de setup:
   - Abra `notebooks/00_setup_cluster.py`
   - Conecte ao cluster configurado
   - Execute todas as células
   - Reinicie o cluster para aplicar as alterações

2. Execute o pipeline principal:
   - Abra `notebooks/01_main_pipeline.py`
   - Conecte ao cluster configurado
   - Execute todas as células

## Tabelas Delta

### Tabelas de Origem
- `bol.feed_varejo_vtex`: Produtos da Bemol

### Tabelas Geradas
- `bol.raw_magalu_products`: Produtos brutos do Magazine Luiza
- `bol.processed_magalu_products`: Produtos processados do Magazine Luiza
- `bol.product_matches`: Produtos correspondentes entre as lojas

## Configurações

### Email
- API Key do SendGrid configurada em `src/config.py`
- Destinatários configurados em `EMAIL_CONFIG`

### Processamento
- Threshold de similaridade: 0.7
- Modelo de embeddings: all-MiniLM-L6-v2
- Batch size: 1000

## Troubleshooting

1. **Erro de Biblioteca**:
   - Execute o notebook `00_setup_cluster.py`
   - Verifique se todas as bibliotecas foram instaladas
   - Reinicie o cluster

2. **Erro de Permissão**:
   - Verificar acesso ao catálogo `bol`
   - Confirmar permissões no DBFS

3. **Erro de Memória**:
   - Aumentar número de workers
   - Ajustar configurações de memória do cluster

4. **Erro de Conexão**:
   - Verificar token do Databricks
   - Confirmar configurações de rede
