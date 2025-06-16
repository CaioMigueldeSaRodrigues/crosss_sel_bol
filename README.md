# Scraping Benchmarking

Projeto para benchmarking de preços entre marketplaces usando processamento de texto e similaridade de embeddings.

## 🚀 Funcionalidades

- Processamento de texto com NLTK
- Geração de embeddings com Sentence Transformers
- Cálculo de similaridade usando cosine similarity
- Limpeza e padronização de preços
- Classificação de similaridade em níveis
- Cálculo de diferença percentual entre preços
- Integração com Databricks

## 📦 Estrutura do Projeto

```
scraping_benchmarking/
├── notebooks/
│   └── 00_setup_cluster.py
├── src/
│   └── scraping/
│       └── magalu.py
├── benchmarking_improved.py
├── benchmarking_databricks.py
└── requirements.txt
```

## 🔧 Configuração

1. Instale as dependências:
```bash
pip install -r requirements.txt
```

2. Para ambiente Databricks:
- Execute o notebook `00_setup_cluster.py` para configurar o cluster
- O notebook instala apenas as bibliotecas necessárias que não estão presentes no cluster

## 💻 Uso

### Versão Local
```python
from benchmarking_improved import BenchmarkingProcessor, TextProcessor, SimilarityProcessor, PriceProcessor

# Inicialize os processadores
text_processor = TextProcessor()
similarity_processor = SimilarityProcessor()
price_processor = PriceProcessor()
benchmarking_processor = BenchmarkingProcessor(
    text_processor, 
    similarity_processor, 
    price_processor
)

# Processe os dados
df_final = benchmarking_processor.process_data(df_magalu, df_bemol)
```

### Versão Databricks
```python
# Execute o notebook benchmarking_databricks.py
# Os resultados serão salvos como:
# 1. TempView: tempview_benchmarking_pares
# 2. Arquivo Parquet: /dbfs/mnt/datalake/silver/benchmarking/benchmarking_results.parquet
```

## 📊 Classificação de Similaridade

- **Exclusivo**: score = -1
- **Muito Similar**: score >= 0.85
- **Moderadamente Similar**: score >= 0.5
- **Pouco Similar**: score < 0.5

## 💰 Processamento de Preços

- Suporte para múltiplos formatos:
  - Formato brasileiro: 5.886,00
  - Formatos mistos: R$ 5.886,00 ou 5.886,00
  - Formatos simples: 5886,00
- Limpeza automática de caracteres especiais
- Conversão para float

## 🔍 Cálculo de Diferença Percentual

- Calculado apenas para pares com similaridade >= 0.90
- Fórmula: |p1 - p2| / ((p1 + p2) / 2) * 100
- Resultado formatado com 2 casas decimais

## 📈 Melhorias Recentes

1. **Processamento de Preços**:
   - Regex mais preciso para formato brasileiro
   - Limpeza básica como fallback
   - Melhor tratamento de erros

2. **Classificação de Similaridade**:
   - Novos thresholds: 0.85 e 0.5
   - Categorias mais descritivas
   - Melhor separação dos níveis

3. **Processamento de Dados**:
   - Cálculo de similaridade em batch
   - Uso de `itertuples` para melhor performance
   - Remoção de processamento redundante

4. **Estrutura do Código**:
   - Injeção de dependências nos processadores
   - Métodos mais focados e coesos
   - Melhor organização do fluxo

5. **Melhorias de Performance**:
   - Cálculo de similaridade otimizado
   - Processamento em batch quando possível
   - Redução de operações redundantes

6. **Salvamento de Dados**:
   - TempView para consultas SQL
   - Parquet para armazenamento permanente
   - Melhor organização dos arquivos

## 🤝 Contribuição

1. Faça um fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📝 Licença

Este projeto está sob a licença MIT. Veja o arquivo [LICENSE](LICENSE) para mais detalhes.
