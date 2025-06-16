# Sistema de Recomendação de Produtos - Databricks

Este projeto implementa um sistema de recomendação de produtos rodando no Databricks, combinando múltiplas estratégias de recomendação:
- Recomendações de cross-selling baseadas em padrões de compra
- Recomendações de produtos similares dentro da mesma categoria
- Recomendações de produtos em promoção

## Estrutura do Projeto

```
├── notebooks/                    # Notebooks do Databricks
│   ├── 01_ingestao_dados/       # Carregamento e pré-processamento de dados
│   ├── 02_engenharia_features/  # Criação e transformação de features
│   └── 03_recomendacoes/        # Geração de recomendações
├── src/                         # Código fonte
│   ├── data/                    # Módulos de processamento de dados
│   ├── models/                  # Modelos de recomendação
│   └── utils/                   # Funções utilitárias
├── sql/                         # Consultas SQL
│   ├── cross_selling/          # Consultas relacionadas a cross-selling
│   └── promocoes/              # Consultas de produtos promocionais
└── config/                      # Arquivos de configuração
```

## Funcionalidades

1. **Recomendações de Cross-Selling**
   - Analisa padrões de compra para identificar produtos frequentemente comprados juntos
   - Utiliza mineração de regras de associação para gerar recomendações

2. **Recomendações de Produtos Similares**
   - Recomenda produtos da mesma categoria
   - Considera atributos do produto e preferências do cliente

3. **Recomendações Promocionais**
   - Integra com dados promocionais de planilhas Excel
   - Recomenda produtos atualmente em promoção

## Stack Tecnológico

- **Plataforma**: Databricks
- **Linguagens**: 
  - Python
  - PySpark
  - SQL
- **Fontes de Dados**:
  - Tabelas do Databricks
  - Arquivos Excel (Promoções)
  - Integração com OneDrive

## Configuração e Setup

1. **Ambiente Databricks**
   - Garantir acesso ao workspace do Databricks necessário
   - Configurar permissões necessárias para acesso ao OneDrive

2. **Dependências**
   - Instalar pacotes Python necessários
   - Configurar bibliotecas do Databricks

3. **Acesso a Dados**
   - Configurar conexões com as fontes de dados necessárias
   - Configurar autenticação para acesso ao OneDrive

## Uso

1. **Ingestão de Dados**
   - Executar os notebooks de ingestão para carregar e pré-processar dados
   - Garantir que todas as tabelas necessárias estejam disponíveis

2. **Geração de Recomendações**
   - Executar os notebooks de recomendação
   - Monitorar os resultados no dashboard do Databricks

3. **Atualizações Promocionais**
   - Atualizar dados promocionais na planilha Excel
   - Executar o processo de recomendação promocional

## Contribuição

1. Faça um fork do repositório
2. Crie uma branch para sua feature
3. Faça commit das suas alterações
4. Push para a branch
5. Abra um Pull Request

## Licença

Este projeto é proprietário e confidencial.

## Contato

Para dúvidas e suporte, entre em contato com a equipe de desenvolvimento. 