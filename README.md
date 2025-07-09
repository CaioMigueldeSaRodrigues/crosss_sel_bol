# Product Recommendation System

Este projeto implementa um sistema de análise de cesta de compras (market basket analysis) com geração de regras de associação via IA (FP-Growth).

## Requisitos de Ambiente

- **Python:** 3.8.10 (veja também o arquivo `runtime.txt`)
- **Dependências:** As versões das principais bibliotecas estão fixadas em `requirements.txt` para garantir reprodutibilidade.

## Estrutura do Projeto

```
├── run_pipeline.py               # Orquestrador principal do pipeline de análise de cesta
├── src/                         # Código-fonte (market basket, regras de associação, etc.)
│   ├── market_basket.py
│   ├── association_rules.py
│   └── ...
├── config.yaml                   # Configuração de caminhos e parâmetros
├── requirements.txt              # Dependências Python
├── runtime.txt                   # Versão recomendada do Python
└── data/                        # Dados de entrada (definidos em config.yaml)
```

## Como Executar

1. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```

2. **(Opcional) Configure os caminhos dos dados em `config.yaml` se necessário.**

3. **Execute o pipeline:**
   ```bash
   python run_pipeline.py
   ```

   O script irá:
   - Carregar e filtrar os dados de vendas e produtos.
   - Gerar regras de associação automaticamente usando FP-Growth (IA).
   - Calcular métricas de suporte e confiança para as regras geradas.
   - Salvar o relatório final no DBFS.

## Principais Funcionalidades

- **Market Basket Analysis com IA:** Geração automática de regras de associação usando FP-Growth.
- **Relatório detalhado:** Métricas de suporte, confiança e recomendações de compra casada.
- **Pipeline automatizado:** Integração com Spark e DBFS para processamento em larga escala.

## Contribuição

1. Fork o repositório
2. Crie uma branch de feature
3. Commit suas alterações
4. Push para a branch
5. Abra um Pull Request

## Licença

Este projeto é proprietário e confidencial.

## Contato

Para dúvidas e suporte, por favor, entre em contato com a equipe de dados da Bol.

## Testes Automatizados

Para rodar os testes automatizados, execute:

```bash
pytest tests/
```

Certifique-se de que todas as dependências estejam instaladas antes de rodar os testes.

