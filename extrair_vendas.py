import snowflake.connector
import pandas as pd
import os

# CONFIGURAÇÃO DE CONEXÃO (Ajuste se necessário)
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),        # Ou coloque seu usuário direto: "SEU_USER"
    password=os.getenv("SNOWFLAKE_PASS"),    # Ou coloque sua senha direto: "SUA_SENHA"
    account="fsjprd",                        # Ajuste para sua conta se for diferente
    warehouse="COMPUTE_WH",
    database="FSJ_PRD",
    schema="GOLD"
)

print("🚀 Conectado ao Snowflake. Extraindo dados de vendas por faixa etária...")

# SQL que você testou e funcionou
sql = """
SELECT 
  dc.CPF_CNPJ,
  dc.UF,
  dc.SEXO,
  CASE 
    WHEN dc.IDADE < 18 THEN '0-17'
    WHEN dc.IDADE BETWEEN 18 AND 25 THEN '18-25'
    WHEN dc.IDADE BETWEEN 26 AND 35 THEN '26-35'
    WHEN dc.IDADE BETWEEN 36 AND 45 THEN '36-45'
    WHEN dc.IDADE BETWEEN 46 AND 55 THEN '46-55'
    WHEN dc.IDADE BETWEEN 56 AND 65 THEN '56-65'
    WHEN dc.IDADE > 65 THEN '65+'
    ELSE 'Não Informado'
  END AS FAIXA_ETARIA,
  v.VALOR_TOTAL as VENDA_PERIODO,
  v.DATA_VENDA
FROM FSJ_PRD.GOLD.DATAMART_CLIENTES dc
INNER JOIN (
  SELECT CPF_CNPJ, INCLUSAO_DATA as DATA_VENDA, SUM(VALOR_TOTAL) AS VALOR_TOTAL
  FROM FSJ_PRD.GOLD.VW_ANALISE_VENDAS
  WHERE INCLUSAO_DATA >= '2026-01-01' -- Pegando o ano todo para o App ser dinâmico
    AND CPF_CNPJ IS NOT NULL
  GROUP BY CPF_CNPJ, INCLUSAO_DATA
) v ON dc.CPF_CNPJ = v.CPF_CNPJ
"""

df = pd.read_sql(sql, conn)

# Salva o arquivo que o App vai ler
df.to_parquet("base_vendas_consolidada.parquet", index=False)

print(f"✅ Sucesso! Arquivo 'base_vendas_consolidada.parquet' gerado com {len(df)} linhas.")
print("Agora o App poderá filtrar o LTV corretamente por mês.")
