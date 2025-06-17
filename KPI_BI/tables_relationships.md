### KPI_BI: Indexação de Tabelas, Relações e Filtros

Este documento detalha as três (3) tabelas principais do projeto, suas relações e os filtros aplicados, servindo como base para o desenvolvimento do dashboard de "Análise de Cesta" (Basket Analysis) no BI.

---

#### 1. Tabelas Principais do Projeto

*   **`hive_metastore.mawe_gold.cross_regras_varejo`**:
    *   **Descrição**: Esta tabela é a fonte das regras de associação (antecedente e consequente) e, presumivelmente, das métricas de suporte e confiança para a análise de cesta. É a base para identificar quais produtos são frequentemente comprados juntos.
    *   **Colunas Relevantes**: `antecedent`, `consequent`, `DATA_EXTRACAO`.
    *   **Uso no Dashboard**: Base para as colunas "Produto" (antecedente) e "Combinação de itens" (consequente), e métricas de combinação.

*   **`bol.faturamento_centros_bol`**:
    *   **Descrição**: Contém os registros detalhados de faturamento e transações de cada item. É utilizada para obter informações sobre os produtos comprados e para calcular as métricas individuais de compra.
    *   **Colunas Relevantes**: `MATERIAL` (ID do produto), `DESCRICAO` (descrição do item na fatura), `PEDIDO` (ID do pedido), `VALOR_LIQUIDO`, `QTD_ITEM` (quantidade do item na transação), `DT_FATURAMENTO` (data da transação), `CLI` (ID do cliente), `CATEGORIA`.
    *   **Uso no Dashboard**: Fornece o ID e a descrição do `Produto` e é usada para calcular o "Número de compras do item" individualmente. Permite filtros por data, categoria e por cliente.

*   **`bol.produtos_site`**:
    *   **Descrição**: Tabela mestre de produtos com informações detalhadas como descrição, categoria, preço e estoque. Usada para enriquecer os IDs de produtos (antecedentes e consequentes) com nomes legíveis e outras propriedades.
    *   **Colunas Relevantes**: `MATERIAL` (ID do produto), `DESCRICAO_MATERIAL` (nome oficial do produto), `CATEGORIA`, `GRUPO_MERCADORIA`, `PRECO`, `PRECO_PROM` (se em promoção), `QUANTIDADE_ESTOQUE`, `CENTRO`.
    *   **Uso no Dashboard**: Fornece o nome completo para "Produto" e "Combinação de itens". Essencial para filtros por categoria, grupo de mercadoria, estoque ou preço.

---

#### 2. Relações entre as Tabelas

As relações são estabelecidas via JOINs para conectar as informações e gerar as métricas do dashboard:

1.  **`cross_regras_varejo` JOIN `faturamento_centros_bol` (via `PEDIDO` e `MATERIAL`)**:
    *   **Condição Principal**: `cross_regras_varejo.antecedent = faturamento_centros_bol.MATERIAL`
    *   **Condição Adicional (para rastreamento do CLI)**: `cross_regras_varejo.PEDIDO = faturamento_centros_bol.PEDIDO` (Esta junção é crucial para rastrear o cliente (`CLI`) associado ao pedido que gerou a regra de associação, permitindo análises de perfil de cliente para as recomendações).
    *   **Propósito**: Conectar as regras de associação aos detalhes do faturamento para o produto antecedente e permitir o rastreamento do cliente que realizou a compra.

2.  **JOIN `cross_regras_varejo` com `produtos_site` (para descrições)**:
    *   **Primeira Junção**: `cross_regras_varejo.antecedent = produtos_site.MATERIAL` (para obter a descrição do `Produto`).
    *   **Segunda Junção**: `cross_regras_varejo.consequent = produtos_site.MATERIAL` (para obter a descrição da `Combinação de itens`).
    *   **Propósito**: Traduzir os IDs de produtos (numéricos) em nomes de produtos legíveis para o dashboard.

---

#### 3. Filtros Essenciais para o Dashboard

Os filtros garantem a relevância e a qualidade dos dados exibidos no dashboard:

*   **Filtro em `hive_metastore.mawe_gold.cross_regras_varejo`**:
    *   `WHERE DATA_EXTRACAO = (SELECT MAX(DATA_EXTRACAO) FROM hive_metastore.mawe_gold.cross_regras_varejo)`:
        *   **Propósito**: Garantir que as regras de associação mais recentes e relevantes sejam utilizadas para a análise.

*   **Filtros em `bol.faturamento_centros_bol` (aplicados na ingestão de dados e no BI)**:
    *   `DT_FATURAMENTO >= current_date() - 90`:
        *   **Propósito**: Limitar a análise a transações recentes (últimos 90 dias), focando em padrões de compra atuais.
    *   `CATEGORIA = 'VAREJO'`:
        *   **Propósito**: Focar a análise em transações de produtos da categoria 'VAREJO', que são relevantes para o escopo do projeto.

*   **Filtros em `bol.produtos_site` (aplicados na ingestão de dados e no BI)**:
    *   `QUANTIDADE_ESTOQUE > 4`:
        *   **Propósito**: Incluir apenas produtos com estoque suficiente, relevantes para recomendações que podem ser atendidas.
    *   `CENTRO = 102`:
        *   **Propósito**: Focar em produtos de um centro de distribuição específico, garantindo a relevância geográfica ou logística das recomendações.
    *   `CATEGORIA = 'VAREJO'`:
        *   **Propósito**: Incluir apenas produtos que pertencem à categoria 'VAREJO'.
    *   `GRUPO_MERCADORIA IS NOT NULL`:
        *   **Propósito**: Excluir produtos sem um grupo de mercadoria definido, garantindo a qualidade e categorização dos dados.

---

#### 4. Mapeamento para as Colunas do Dashboard (Exemplo)

Assumindo que a tabela `cross_regras_varejo` já contém informações sobre as ocorrências de combinações, ou que estas serão calculadas no próximo passo do pipeline, as colunas do dashboard podem ser mapeadas da seguinte forma:

*   **`Produto`**:
    *   `cross_regras_varejo.antecedent` (com `DESCRICAO_MATERIAL` de `bol.produtos_site`).

*   **`Combinação de itens`**:
    *   `cross_regras_varejo.consequent` (com `DESCRICAO_MATERIAL` de `bol.produtos_site`).

*   **`Número de compras do item`**:
    *   Agregação de `COUNT(faturamento_centros_bol.PEDIDO)` ou `COUNT(faturamento_centros_bol.MATERIAL)` para o `antecedent`.

*   **`Número de compras da combinação`**:
    *   Métrica diretamente da `cross_regras_varejo` (se disponível) ou calculada a partir da contagem de pares (`antecedent`, `consequent`) nos dados de faturamento.

*   **`% Nas compras do item individual`**:
    *   Cálculo: `(Número de compras da combinação / Número de compras do item) * 100`.

---

Este detalhamento deve fornecer uma base sólida para a construção do seu dashboard no BI, garantindo a consistência com as fontes de dados do projeto no Databricks. 