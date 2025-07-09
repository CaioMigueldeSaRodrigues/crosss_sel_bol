# Product Recommendation System

Este projeto implementa um sistema de análise de cesta de compras (market basket analysis) com geração de regras de associação via IA (FP-Growth) e exportação de KPIs otimizados para Power BI.

## Requisitos de Ambiente

- **Python:** 3.8.10 (veja também o arquivo `runtime.txt`)
- **Dependências:** As versões das principais bibliotecas estão fixadas em `requirements.txt` para garantir reprodutibilidade.
- **Spark:** O pipeline utiliza PySpark para processamento distribuído.

## Estrutura do Projeto

```
├── run_pipeline.py               # Orquestrador principal do pipeline de análise de cesta e KPIs
├── src/                         # Código-fonte (market basket, regras de associação, etc.)
│   ├── market_basket.py
│   ├── association_rules.py
│   └── ...
├── KPI_BI/                      # Módulo para geração de KPIs para Power BI
│   ├── __init__.py
│   └── power_bi_export.py
├── config.yaml                   # Configuração centralizada de tabelas, filtros, caminhos e parâmetros de IA
├── requirements.txt              # Dependências Python
├── runtime.txt                   # Versão recomendada do Python
└── data/                        # Dados de entrada (definidos em config.yaml)
```

## Como Executar

1. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure todos os parâmetros e caminhos em `config.yaml`:**
   - Exemplo de seções:
     - tabelas: nomes das tabelas de faturamento e produtos
     - filtros: centro, categoria, quantidade mínima
     - caminho_saida: caminho para salvar o relatório de regras
     - caminho_saida_kpi: caminho para salvar os KPIs otimizados para Power BI
     - modelo_ia: parâmetros do FP-Growth (min_support, min_confidence)

3. **Execute o pipeline:**
   ```bash
   python run_pipeline.py
   ```

   O script irá:
   - Carregar e filtrar os dados de vendas e produtos conforme o config.yaml
   - Gerar regras de associação automaticamente usando FP-Growth (IA)
   - Calcular métricas de suporte, confiança e **Lift** para as regras geradas
   - Gerar uma tabela de KPIs otimizada para Power BI (com Lift)
   - Salvar o relatório de regras e os KPIs nos caminhos definidos no config.yaml

## Principais Funcionalidades

- **Market Basket Analysis com IA:** Geração automática de regras de associação usando FP-Growth.
- **Relatório detalhado:** Métricas de suporte, confiança, Lift e recomendações de compra casada.
- **Pipeline automatizado:** Integração com Spark e DBFS para processamento em larga escala.
- **Exportação para Power BI:** KPIs otimizados para dashboards, incluindo a métrica de Lift.
- **Configuração centralizada:** Todos os parâmetros e caminhos relevantes são definidos no config.yaml.

## Sobre a Métrica de Lift

O **Lift** indica o quanto a ocorrência do item consequente é mais provável dado que o antecedente ocorreu, em relação ao acaso. Valores de Lift maiores que 1 indicam forte associação entre os itens.

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

