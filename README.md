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
│   │   └── embeddings.py      # Lógica de geração de embeddings
│   ├── analysis/
│   │   └── similarity.py      # Lógica de similaridade e pareamento
│   ├── export/
│   │   └── export_excel.py
│   ├── email/
│   │   └── send_email.py
│   └── config.py              # Configurações globais
└── requirements.txt
```

## Iniciando o Projeto

### 1. Configuração do Cluster

Para melhor desempenho e compatibilidade, utilize as seguintes configurações no seu cluster Databricks (Cluster ID: `0521-212707-5nlw5qu4`, Nome: `DATA-ONLINE-01`):
   - **Runtime**: `16.2.x-scala2.12` (ou versão compatível com Apache Spark 3.5.0 e Delta Lake 3.0.0)
   - **Tipo de Nó (Driver)**: `Standard_D16_v3`
   - **Tipo de Nó (Workers)**: `Standard_DS3_v2`
   - **Auto-escalonamento**: Mínimo de 1 worker, Máximo de 4 workers.
   - **Modo de Acesso**: `User Isolation` (ou Compartilhado)
   - **Bibliotecas**: As bibliotecas Python necessárias serão instaladas automaticamente pelo notebook `00_setup_cluster.py` usando `%pip install --upgrade --force-reinstall`. Certifique-se de que o cluster tenha acesso à internet para download das dependências.

### 2. Configuração do Repositório

1. Clone o repositório no Databricks:
   - Vá para "Repos" no menu lateral
   - Clique em "Add Repo"
   - Cole a URL: `https://github.com/CaioMigueldeSaRodrigues/scraping_benchmarking.git`
   - Dê um nome para o repositório (ex: "scraping_benchmarking")

### 3. Execução do Pipeline

1. **Execute o notebook de setup:**
   - Abra `notebooks/00_setup_cluster.py`
   - Conecte ao cluster configurado
   - **Execute TODAS as células.** Este notebook garante que o ambiente Python esteja configurado corretamente (incluindo o `sys.path` e todas as bibliotecas).
   
2. **Execute o pipeline principal:**
   - Abra `notebooks/01_main_pipeline.py`
   - Conecte ao cluster configurado
   - **Execute TODAS as células.** A `SparkSession` será automaticamente injetada pelo Databricks, e o pipeline usará as funções otimizadas.

## Tabelas Delta

### Tabelas de Origem
- `bol.feed_varejo_vtex`: Produtos da Bemol (verifique se esta tabela existe no seu Unity Catalog/Hive Metastore)

### Tabelas Geradas
- `bol.raw_magalu_products`: Produtos brutos do Magazine Luiza
- `bol.processed_magalu_products`: Produtos processados do Magazine Luiza
- `bol.product_matches`: Produtos correspondentes entre as lojas

## Configurações

### Email
- API Key do SendGrid configurada em `src/config.py` (Variável de ambiente `SENDGRID_API_KEY` é recomendada)
- Destinatários configurados em `EMAIL_CONFIG`

### Processamento
- Threshold de similaridade: 0.7
- Modelo de embeddings: `all-MiniLM-L6-v2`
- **Geração de Embeddings**: A lógica foi otimizada em `src/processing/embeddings.py` para converter os dados para Pandas, gerar embeddings localmente com `SentenceTransformer`, e depois converter de volta para Spark DataFrame. Isso garante melhor compatibilidade e desempenho em clusters Databricks com modo de acesso compartilhado.

## Troubleshooting

1.  **`ImportError` ou `JVM_ATTRIBUTE_NOT_SUPPORTED`**:
    *   Certifique-se de que o cluster está configurado conforme o item "1. Configuração do Cluster".
    *   **Execute SEMPRE** o `notebooks/00_setup_cluster.py` **com todas as células** antes de executar o pipeline principal. O comando `!pip install --upgrade --force-reinstall` é crucial para resolver problemas de carregamento de módulos e cache em ambientes Databricks.
    *   O erro `[JVM_ATTRIBUTE_NOT_SUPPORTED]` ocorre em clusters de modo compartilhado ao tentar acessar `sparkContext` diretamente. As linhas ofensivas foram comentadas em `00_setup_cluster.py`.

2.  **Erro de Permissão**:
    *   Verificar acesso ao catálogo `bol` e às tabelas Delta.
    *   Confirmar permissões de escrita nos diretórios do DBFS (`/FileStore/tables/`).

3.  **Erro de Memória / Desempenho**:
    *   Aumentar o número máximo de workers no auto-escalonamento do cluster.
    *   Ajustar configurações de memória do cluster, se necessário.

4.  **Erro de Conexão (Scraping)**:
    *   Verificar a conectividade de rede do cluster para a internet (sites do Magazine Luiza).
    *   Confirmar que o cluster não está por trás de um proxy que impede as requisições.
