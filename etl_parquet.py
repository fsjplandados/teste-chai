import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime
import os
import glob

DSN = 'SNOWFLAKE_FSJ'

def run_etl():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando extração (Filtro: Atividade desde 01/2025)...")
    try:
        conn = pyodbc.connect(f"DSN={DSN}", autocommit=True)
    except Exception as e:
        print(f"Erro ao conectar ao Snowflake: {e}")
        return
    
    query = """
    WITH VENDAS_AGG AS (
        SELECT 
            CPF_CNPJ,
            MAX(CASE WHEN TIPO_VENDA_DESCRICAO IN ('Venda Caixa', 'Venda Balcão', 'Auto Atendimento') THEN INCLUSAO_DATA END) AS ULTIMA_COMPRA_LOJA,
            MAX(CASE WHEN TIPO_VENDA_DESCRICAO IN ('E-commerce', 'APP', 'SITE', 'Rappi', 'iFood') THEN INCLUSAO_DATA END) AS ULTIMA_COMPRA_DIGITAL,
            MAX(CASE WHEN TIPO_VENDA_DESCRICAO IN ('Venda Tele Entrega', 'APP Tele Entrega', 'SITE Tele Entrega', 'Tele Vizinhança', 'Tele Encaminhada Lojas', 'Venda Tele Entrega Central') THEN INCLUSAO_DATA END) AS ULTIMA_COMPRA_OMNI
        FROM FSJ_PRD.GOLD.VW_ANALISE_VENDAS_BASE
        WHERE INCLUSAO_DATA >= '2025-01-01'
        GROUP BY CPF_CNPJ
    )
    SELECT
        c.PRIMEIRA_COMPRA::DATE AS PRIMEIRA_COMPRA,
        c.ULTIMA_COMPRA::DATE AS ULTIMA_COMPRA_GERAL,
        v.ULTIMA_COMPRA_LOJA::DATE AS ULTIMA_COMPRA_LOJA,
        v.ULTIMA_COMPRA_DIGITAL::DATE AS ULTIMA_COMPRA_DIGITAL,
        v.ULTIMA_COMPRA_OMNI::DATE AS ULTIMA_COMPRA_OMNI,
        CAST(c.VALOR_TOTAL AS FLOAT) AS VALOR_TOTAL,
        CAST(c.TOTAL_COMPRAS AS INT) AS TOTAL_COMPRAS,
        L.UF_CIDADE AS UF,
        L.NOME_CIDADE AS CIDADE,
        L.LOJA_NOME AS LOJA,
        L.REGIAO_NOME AS REGIAO,
        c.SEXO,
        CASE 
            WHEN c.IDADE < 24 THEN 'Menor de 24'
            WHEN c.IDADE BETWEEN 25 AND 34 THEN 'Entre 25 e 34'
            WHEN c.IDADE BETWEEN 35 AND 44 THEN 'Entre 35 e 44'
            WHEN c.IDADE BETWEEN 45 AND 54 THEN 'Entre 45 e 54'
            WHEN c.IDADE BETWEEN 55 AND 64 THEN 'Entre 55 e 64'
            WHEN c.IDADE >= 65 THEN 'Mais de 65'
            ELSE 'Não Informado'
        END AS FAIXA_ETARIA,
        c.TIPO_PESSOA
    FROM FSJ_PRD.GOLD.DATAMART_CLIENTES c
    LEFT JOIN FSJ_PRD.GOLD.VW_LOJAS L ON c.LOJA_ID_PREFERENCIA = L.LOJA_ID
    INNER JOIN VENDAS_AGG v ON c.CPF_CNPJ = v.CPF_CNPJ
    WHERE c.ULTIMA_COMPRA >= '2025-01-01'
    """
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Baixando dados do Snowflake...")
    try:
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        print(f"Erro ao executar query: {e}")
        conn.close()
        return
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Otimizando {len(df)} linhas...")
    cat_cols = ['UF', 'CIDADE', 'LOJA', 'REGIAO', 'SEXO', 'FAIXA_ETARIA', 'TIPO_PESSOA']
    for col in cat_cols:
        df[col] = df[col].astype('category')
        
    # Limpar arquivos antigos
    for f in glob.glob("base_crm_p*.parquet"):
        os.remove(f)
    if os.path.exists("base_crm.parquet"):
        os.remove("base_crm.parquet")

    # Dividir em 2 partes para o GitHub aceitar (limite de 100MB por arquivo)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Dividindo em 2 partes e salvando...")
    mid = len(df) // 2
    df.iloc[:mid].to_parquet('base_crm_p1.parquet', engine='pyarrow', compression='brotli')
    df.iloc[mid:].to_parquet('base_crm_p2.parquet', engine='pyarrow', compression='brotli')
    
    s1 = os.path.getsize('base_crm_p1.parquet') / (1024*1024)
    s2 = os.path.getsize('base_crm_p2.parquet') / (1024*1024)
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Sucesso! Partes salvas: {s1:.2f} MB e {s2:.2f} MB.")
    print("\n==================================================================")
    print(" PRONTO! Agora os arquivos estão pequenos o suficiente para o GitHub.")
    print(" Suba os dois arquivos: base_crm_p1.parquet e base_crm_p2.parquet")
    print("==================================================================")

if __name__ == '__main__':
    run_etl()
