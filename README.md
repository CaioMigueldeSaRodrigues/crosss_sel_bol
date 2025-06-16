# Benchmarking de Preços - Databricks

Este projeto implementa um pipeline completo de benchmarking de preços no ambiente Databricks, integrando scraping, processamento de dados, análise de similaridade e geração de relatórios.

## Estrutura do Projeto

```
.
├── benchmarking_databricks_unified.py  # Código principal unificado
├── requirements.txt                    # Dependências do projeto
├── README.md                          # Documentação
└── notebooks/                         # Notebooks Databricks
    └── 00_setup_cluster.py           # Configuração do cluster
```

## Funcionalidades

1. **Scraping de Produtos**
   - Extração automatizada de produtos do Magazine Luiza
   - Tratamento robusto de preços e nomes
   - Geração de IDs únicos

2. **Processamento de Dados**
   - Limpeza e normalização de preços
   - Processamento de texto com NLTK
   - Geração de embeddings com SentenceTransformer

3. **Análise de Similaridade**
   - Cálculo de similaridade cosseno
   - Identificação de pares de produtos similares
   - Classificação de níveis de similaridade

4. **Geração de Relatórios**
   - Exportação para DBFS
   - Geração de relatórios HTML
   - Criação de TempViews para consultas

## Requisitos

- Python 3.8+
- Databricks Runtime 10.4+
- Bibliotecas listadas em `requirements.txt`

## Instalação

1. Clone o repositório
2. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

## Uso

1. Configure o cluster Databricks usando o notebook `00_setup_cluster.py`
2. Execute o pipeline principal:
   ```python
   from benchmarking_databricks_unified import main
   main()
   ```

## Configuração

O código utiliza as seguintes configurações padrão:
- Threshold de similaridade: 0.7
- Número máximo de páginas para scraping: 5
- Modelo de embeddings: 'all-MiniLM-L6-v2'

## Logs e Monitoramento

- Logs são gerados em tempo real
- Erros são capturados e registrados
- Métricas de performance são monitoradas

## Contribuição

1. Fork o projeto
2. Crie uma branch para sua feature
3. Commit suas mudanças
4. Push para a branch
5. Abra um Pull Request

## Licença

Este projeto está licenciado sob a MIT License - veja o arquivo LICENSE para detalhes.
