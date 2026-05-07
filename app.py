import streamlit as st
import pandas as pd
import duckdb
from datetime import datetime, timedelta, date
import os
import glob

# Configuração da página
st.set_page_config(page_title="Dashboard CRM - Farmácias São João", layout="wide", initial_sidebar_state="expanded")

# CSS customizado para os cartões bonitos
st.markdown("""
<style>
    .metric-card {
        background-color: white;
        border: 1px solid #E5E7EB;
        border-radius: 16px;
        padding: 24px;
        position: relative;
        overflow: hidden;
        box-shadow: 0 2px 10px rgba(0,0,0,.03);
    }
    .metric-card::before {
        content: ''; position: absolute; top: 0; left: 0; width: 100%; height: 4px;
    }
    .c-purple::before { background: linear-gradient(90deg, #7C3AED, #a78bfa); }
    .c-green::before { background: linear-gradient(90deg, #10B981, #34d399); }
    .c-blue::before { background: linear-gradient(90deg, #0EA5E9, #38bdf8); }
    .c-orange::before { background: linear-gradient(90deg, #F97316, #fb923c); }
    .c-gray::before { background: #9CA3AF; }
    
    .lbl { font-size: 12px; font-weight: 700; color: #6B7280; text-transform: uppercase; margin-bottom: 8px; }
    .val { font-size: 32px; font-weight: 800; color: #111827; margin-bottom: 4px; line-height: 1; }
    .desc { font-size: 12px; color: #9CA3AF; }
</style>
""", unsafe_allow_html=True)

# --- CONEXÃO DUCKDB ---
@st.cache_resource
def get_con():
    c = duckdb.connect(database=':memory:')
    # Registramos os arquivos como uma VIEW para facilitar o acesso
    c.execute("CREATE OR REPLACE VIEW dados AS SELECT * FROM read_parquet('base_crm_p*.parquet')")
    return c

con = get_con()

# --- PREPARAÇÃO DE FILTROS (LOOKUP TABLE) ---
@st.cache_data(ttl="1d")
def carregar_lookup():
    try:
        # Usamos a VIEW 'dados' que já foi registrada
        return con.execute("SELECT DISTINCT UF, CIDADE, LOJA, REGIAO, TIPO_PESSOA, SEXO FROM dados").df()
    except Exception as e:
        st.error(f"Erro ao carregar lookup: {e}")
        return pd.DataFrame()

df_lookup = carregar_lookup()

# --- BARRA LATERAL (FILTROS) ---
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/4/4b/Logo_Farmacias_Sao_Joao.png", width=150)
st.sidebar.title("Filtros CRM")

col1, col2 = st.sidebar.columns(2)
data_inicio = col1.date_input("Data Início", value=date(2025, 1, 1))
data_termino = col2.date_input("Data Término", value=date.today())

canal = st.sidebar.selectbox("Canal de Venda", ["Total", "Loja", "Digital", "Omnichannel"])

st.sidebar.markdown("---")
st.sidebar.subheader("Demográficos")

def get_opts(col, f_col=None, f_val=None):
    if df_lookup.empty: return ["Todas"]
    if f_col and f_val and f_val != "Todas":
        return ["Todas"] + sorted(df_lookup[df_lookup[f_col] == f_val][col].dropna().unique().tolist())
    return ["Todas"] + sorted(df_lookup[col].dropna().unique().tolist())

uf_sel = st.sidebar.selectbox("UF da Loja", get_opts('UF'))
cid_sel = st.sidebar.selectbox("Cidade da Loja", get_opts('CIDADE', 'UF', uf_sel))
loj_sel = st.sidebar.selectbox("Loja", get_opts('LOJA', 'CIDADE', cid_sel) if cid_sel != "Todas" else get_opts('LOJA', 'UF', uf_sel))
reg_sel = st.sidebar.selectbox("Região", get_opts('REGIAO'))
faixa_etaria = st.sidebar.selectbox("Faixa Etária", ["Todas", "Menor de 24", "Entre 25 e 34", "Entre 35 e 44", "Entre 45 e 54", "Entre 55 e 64", "Mais de 65"])
sexo = st.sidebar.selectbox("Sexo", ["Todos"] + [s for s in get_opts('SEXO') if s != "Todas"])
tipo_cliente = st.sidebar.selectbox("Tipo de Cliente", get_opts('TIPO_PESSOA'))

# --- CÁLCULO DE MÉTRICAS COM DUCKDB ---
def calcular_metricas():
    def esc(t): return str(t).replace("'", "''")
    
    where_clauses = ["1=1"]
    if uf_sel != "Todas": where_clauses.append(f"UF = '{esc(uf_sel)}'")
    if cid_sel != "Todas": where_clauses.append(f"CIDADE = '{esc(cid_sel)}'")
    if loj_sel != "Todas": where_clauses.append(f"LOJA = '{esc(loj_sel)}'")
    if reg_sel != "Todas": where_clauses.append(f"REGIAO = '{esc(reg_sel)}'")
    if faixa_etaria != "Todas": where_clauses.append(f"FAIXA_ETARIA = '{esc(faixa_etaria)}'")
    if sexo != "Todos": where_clauses.append(f"SEXO = '{esc(sexo)}'")
    if tipo_cliente != "Todos": where_clauses.append(f"TIPO_PESSOA = '{esc(tipo_cliente)}'")
    
    where_str = " AND ".join(where_clauses)
    
    col_dt = "ULTIMA_COMPRA_GERAL"
    if canal == 'Loja': col_dt = "ULTIMA_COMPRA_LOJA"
    elif canal == 'Digital': col_dt = "ULTIMA_COMPRA_DIGITAL"
    elif canal == 'Omnichannel': col_dt = "ULTIMA_COMPRA_OMNI"
    
    limite_ativos = data_termino - timedelta(days=90)

    sql = f"""
    SELECT 
        COUNT(*) as total,
        COUNT(PRIMEIRA_COMPRA) as ident,
        SUM(CASE WHEN PRIMEIRA_COMPRA >= '{data_inicio}' AND PRIMEIRA_COMPRA <= '{data_termino}' THEN 1 ELSE 0 END) as novos,
        SUM(CASE WHEN {col_dt} >= '{limite_ativos}' AND {col_dt} <= '{data_termino}' THEN 1 ELSE 0 END) as ativos,
        AVG(VALOR_TOTAL) as ltv,
        SUM(VALOR_TOTAL) / NULLIF(SUM(TOTAL_COMPRAS), 0) as ticket
    FROM dados
    WHERE {where_str}
    """
    return con.execute(sql).fetchone()

try:
    res = calcular_metricas()
    qtd_total, qtd_ident, qtd_novos, qtd_ativos, ltv_medio, ticket_medio = res
except Exception as e:
    st.error(f"Erro no SQL: {e}")
    st.stop()

# --- LAYOUT PRINCIPAL ---
num_arquivos = len(glob.glob('base_crm_p*.parquet'))
st.title("📊 Dashboard CRM - Performance de Clientes")
st.caption(f"Status: {num_arquivos} arquivos carregados. Analisando **{qtd_total:,.0f}** clientes.".replace(",", "."))

def fmt_n(v): return f"{int(v or 0):,}".replace(",", ".")
def fmt_b(v): return f"R$ {(v or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

st.markdown("<br>", unsafe_allow_html=True)

html_cards = f"""
<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 24px;">
    <div class="metric-card c-gray"><div class="lbl">Clientes Totais</div><div class="val">{fmt_n(qtd_total)}</div><div class="desc">Base filtrada</div></div>
    <div class="metric-card c-purple"><div class="lbl">Identificados</div><div class="val">{fmt_n(qtd_ident)}</div><div class="desc">Com histórico</div></div>
    <div class="metric-card c-green"><div class="lbl">Ativos (90 dias)</div><div class="val">{fmt_n(qtd_ativos)}</div><div class="desc">Canal {canal}</div></div>
    <div class="metric-card c-blue"><div class="lbl">Novos Clientes</div><div class="val">{fmt_n(qtd_novos)}</div><div class="desc">1ª compra no período</div></div>
    <div class="metric-card c-orange"><div class="lbl">LTV Médio</div><div class="val">{fmt_b(ltv_medio)}</div><div class="desc">Receita por cliente</div></div>
    <div class="metric-card c-purple"><div class="lbl">Ticket Médio</div><div class="val">{fmt_b(ticket_medio)}</div><div class="desc">Por transação</div></div>
</div>
"""
st.markdown(html_cards, unsafe_allow_html=True)
