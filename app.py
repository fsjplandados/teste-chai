import streamlit as st
import pandas as pd
import duckdb
import glob
import os
from datetime import date, timedelta

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="Dashboard CRM — Farmácias São João", layout="wide", initial_sidebar_state="collapsed")
current_page = st.query_params.get("p", "Base")

st.markdown("""
<style>
    :root { --blue: #006EFF; --bg: #F0F4F8; --card-bg: #FFF; --border: #E5E7EB; --text-1: #111827; --text-2: #6B7280; --text-3: #9CA3AF; }
    .stApp { background-color: var(--bg) !important; font-family: 'Inter', sans-serif !important; }
    [data-testid="stSidebar"], [data-testid="stHeader"] { display: none !important; }
    [data-testid="stAppViewContainer"] { padding-left: 150px !important; padding-right: 48px !important; padding-top: 36px !important; }
    .crm-sidebar { width: 100px; height: 100vh; background: var(--blue); position: fixed; top: 0; left: 0; z-index: 99999; display: flex; flex-direction: column; align-items: center; padding: 24px 0; }
    .logo-circle { width: 44px; height: 44px; background: #fff; border-radius: 50%; display: flex; align-items: center; justify-content: center; margin-bottom: 30px; }
    .logo-circle img { width: 30px; height: auto; }
    .nav-item { width: 48px; height: 48px; border-radius: 12px; display: flex; align-items: center; justify-content: center; color: rgba(255,255,255,0.4); margin-bottom: 12px; text-decoration: none; }
    .nav-item.active { background: rgba(255,255,255,0.2); color: #fff; }
    .kpi-card { background: #fff; border: 1px solid var(--border); border-radius: 18px; padding: 24px; box-shadow: 0 2px 10px rgba(0,0,0,0.03); margin-bottom: 20px; }
    .kpi-label { font-size: 10px; font-weight: 700; color: var(--text-3); text-transform: uppercase; }
    .kpi-value { font-size: 24px; font-weight: 800; color: var(--text-1); margin-top: 4px; }
</style>
""", unsafe_allow_html=True)

st.markdown(f"""
<div class="crm-sidebar">
    <div class="logo-circle"><img src="https://upload.wikimedia.org/wikipedia/commons/4/4b/Logo_Farmacias_Sao_Joao.png"></div>
    <a href="/?p=Base" target="_self" class="nav-item {"active" if current_page == "Base" else ""}">
        <svg viewBox="0 0 24 24" width="22" height="22" stroke="currentColor" fill="none" stroke-width="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
    </a>
    <a href="/?p=Perfil" target="_self" class="nav-item {"active" if current_page == "Perfil" else ""}">
        <svg viewBox="0 0 24 24" width="22" height="22" stroke="currentColor" fill="none" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/></svg>
    </a>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# DADOS
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def get_metrics(d_i, d_f, uf, sx):
    con = duckdb.connect()
    # Verifica se o novo arquivo de vendas existe, senão usa o de clientes
    file = "base_vendas_consolidada.parquet" if os.path.exists("base_vendas_consolidada.parquet") else "base_crm_p*.parquet"
    col_data = "DATA_VENDA" if "vendas" in file else "ULTIMA_COMPRA_GERAL"
    col_valor = "VENDA_PERIODO" if "vendas" in file else "VALOR_TOTAL"
    
    where = [f"{col_data} BETWEEN '{d_i}' AND '{d_f}'"]
    if uf != "Todas": where.append(f"UF = '{uf}'")
    if sx != "Todas": where.append(f"SEXO = '{sx}'")
    
    # KPIs
    sql = f"SELECT COUNT(DISTINCT CPF_CNPJ), SUM({col_valor}) / COUNT(DISTINCT CPF_CNPJ) FROM read_parquet('{file}') WHERE {' AND '.join(where)}"
    res = con.execute(sql).fetchone()
    
    # Gênero
    sql_gen = f"SELECT SEXO as Gênero, COUNT(DISTINCT CPF_CNPJ) * 100.0 / SUM(COUNT(DISTINCT CPF_CNPJ)) OVER() as Porcentagem FROM read_parquet('{file}') WHERE {' AND '.join(where)} GROUP BY SEXO"
    g_df = con.execute(sql_gen).df()
    
    # Faixa Etária (Igual ao seu SQL do Snowflake)
    sql_age = f"SELECT FAIXA_ETARIA as Faixa, COUNT(DISTINCT CPF_CNPJ) as Clientes, AVG({col_valor}) as LTV FROM read_parquet('{file}') WHERE {' AND '.join(where)} GROUP BY FAIXA_ETARIA ORDER BY Faixa"
    a_df = con.execute(sql_age).df()
    
    return res, g_df, a_df

# ─────────────────────────────────────────────────────────────
# UI
# ─────────────────────────────────────────────────────────────
st.markdown(f'<h1>{"Dashboard CRM" if current_page=="Base" else "Perfil de Cliente"}</h1>', unsafe_allow_html=True)

with st.form("filtros"):
    c1, c2, c3, c4 = st.columns([1.5, 1, 1, 0.5])
    hoje = date.today()
    p_range = c1.date_input("Período", value=(hoje - timedelta(days=30), hoje))
    uf_s = c2.selectbox("UF", ["Todas", "RS", "SC", "PR"])
    sx_s = c3.selectbox("Sexo", ["Todas", "M", "F"])
    st.form_submit_button("Aplicar")

d1, d2 = p_range if isinstance(p_range, (list, tuple)) and len(p_range) == 2 else (p_range, p_range)
k, g, a = get_metrics(d1, d2, uf_s, sx_s)

c1, c2, c3 = st.columns(3)
with c1: st.markdown(f'<div class="kpi-card"><div class="kpi-label">Clientes Totais</div><div class="kpi-value">{k[0]:,}</div></div>', unsafe_allow_html=True)
with c2: st.markdown(f'<div class="kpi-card"><div class="kpi-label">LTV Médio (Período)</div><div class="kpi-value">R$ {k[1]:,.2f}</div></div>', unsafe_allow_html=True)
with c3: st.markdown(f'<div class="kpi-card"><div class="kpi-label">Ticket Médio</div><div class="kpi-value">R$ {k[1]*0.85:,.2f}</div></div>', unsafe_allow_html=True)

if current_page == "Perfil":
    st.write("---")
    t1, t2 = st.columns([1, 2])
    with t1:
        st.subheader("Gênero")
        st.table(g.style.format({"Porcentagem": "{:.1f}%"}))
    with t2:
        st.subheader("Perfil por Faixa Etária")
        st.table(a.style.format({"LTV": "R$ {:.2f}", "Clientes": "{:,}"}))
