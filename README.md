# Scraping Benchmarking - Databricks

Projeto de benchmarking de preços entre Magazine Luiza e Bemol, executado no Databricks.

## Estrutura do Projeto

```
/Repos/seu_usuario/scraping_benchmarking/
├── notebooks/
│   └── 01_main_pipeline.py
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── scraping/
│   │   ├── __init__.py
│   │   ├── magalu.py
│   │   └── bemol.py
│   ├── processing/
│   │   ├── __init__.py
│   │   ├── cleaning.py
│   │   └── embeddings.py
│   ├── analysis/
│   │   ├── __init__.py
│   │   └── similarity.py
│   ├── export/
│   │   ├── __init__.py
│   │   └── export_excel.py
│   └── email/
│       ├── __init__.py
│       └── send_email.py
└── requirements.txt
```

## Configuração do Ambiente

1. **Cluster Databricks**:
   - Runtime: 13.3 LTS (includes Apache Spark 3.4.1, Scala 2.12)
   - Node Type: Standard_DS3_v2 (ou similar)
   - Min Workers: 1
   - Max Workers: 2

2. **Bibliotecas Necessárias**:
   - sentence-transformers==2.2.2
   - pandas==2.1.4
   - openpyxl==3.1.2
   - beautifulsoup4==4.12.2
   - requests==2.31.0

3. **Permissões Necessárias**:
   - Acesso ao catalog 'bol'
   - Permissões de leitura/escrita nas tabelas
   - Acesso ao DBFS para exportação de arquivos

## Tabelas Delta

O projeto utiliza as seguintes tabelas no catalog 'bol':

1. **Tabelas de Origem**:
   - `bol.feed_varejo_vtex`: Dados dos produtos da Bemol

2. **Tabelas Geradas**:
   - `bol.raw_magalu_products`: Dados brutos do Magazine Luiza
   - `bol.raw_bemol_products`: Dados brutos da Bemol
   - `bol.processed_magalu_products`: Dados processados do Magazine Luiza
   - `bol.processed_bemol_products`: Dados processados da Bemol
   - `bol.product_matches`: Resultados do matching de produtos

## Execução

1. **Notebook Principal**:
   - Localização: `/notebooks/01_main_pipeline.py`
   - Executa o pipeline completo de scraping, processamento e matching

2. **Job Databricks**:
   - Criar job no Databricks Workflows
   - Adicionar notebook principal como tarefa
   - Configurar agendamento (se necessário)

## Configurações

As configurações do projeto estão no arquivo `src/config.py`:

1. **Email**:
   - Configurações SMTP para envio de relatórios
   - Lista de destinatários

2. **Processamento**:
   - Threshold de similaridade para matching
   - Tamanho do batch para processamento

3. **Armazenamento**:
   - Caminhos para exportação de arquivos
   - Configurações de logging

## Troubleshooting

1. **Erro de Permissão**:
   - Verificar acesso ao catalog 'bol'
   - Verificar permissões nas tabelas
   - Verificar acesso ao DBFS

2. **Erro de Memória**:
   - Aumentar número de workers
   - Ajustar tamanho do batch
   - Otimizar queries

3. **Erro de Biblioteca**:
   - Verificar instalação das bibliotecas no cluster
   - Verificar versões compatíveis
   - Reiniciar cluster se necessário
