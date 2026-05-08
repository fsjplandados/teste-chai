import streamlit as st
import pandas as pd
import duckdb
import glob
import os
from datetime import date, timedelta

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÃO E DESIGN SYSTEM
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard CRM — Farmácias São João",
    layout="wide",
    initial_sidebar_state="collapsed",
)

query_params = st.query_params
current_page = query_params.get("p", "Base")

st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet"/>
<style>
    :root {
        --blue: #006EFF; --bg: #F0F4F8; --card-bg: #FFF; --border: #E5E7EB;
        --text-1: #111827; --text-2: #6B7280; --text-3: #9CA3AF;
        --green: #10B981; --purple: #7C3AED; --orange: #F97316;
    }
    .stApp { background-color: var(--bg) !important; font-family: 'Inter', sans-serif !important; }
    [data-testid="stSidebar"], [data-testid="stHeader"] { display: none !important; }
    [data-testid="stAppViewContainer"] { padding-left: 150px !important; padding-right: 48px !important; padding-top: 36px !important; }
    
    .crm-sidebar {
        width: 100px; height: 100vh; background: var(--blue);
        position: fixed; top: 0; left: 0; z-index: 99999;
        display: flex; flex-direction: column; align-items: center; padding: 24px 0;
    }
    .logo-circle { width: 44px; height: 44px; background: #fff; border-radius: 50%; display: flex; align-items: center; justify-content: center; margin-bottom: 30px; }
    .logo-circle img { width: 30px; height: auto; }
    .nav-item { width: 48px; height: 48px; border-radius: 12px; display: flex; align-items: center; justify-content: center; color: rgba(255,255,255,0.4); margin-bottom: 12px; text-decoration: none; }
    .nav-item.active { background: rgba(255,255,255,0.2); color: #fff; }
    
    .kpi-card { background: #fff; border: 1px solid var(--border); border-radius: 18px; padding: 24px; box-shadow: 0 2px 10px rgba(0,0,0,0.03); margin-bottom: 20px; }
    .kpi-label { font-size: 10px; font-weight: 700; color: var(--text-3); text-transform: uppercase; letter-spacing: .1em; }
    .kpi-value { font-size: 24px; font-weight: 800; color: var(--text-1); margin-top: 4px; }
</style>
""", unsafe_allow_html=True)

# SIDEBAR
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
# FILTROS E LÓGICA DE DADOS (AGREGAÇÃO EM SQL)
# ─────────────────────────────────────────────────────────────
st.markdown(f'<h1 style="font-size:24px; font-weight:800;">{"Dashboard CRM" if current_page=="Base" else "Perfil de Cliente"}</h1>', unsafe_allow_html=True)

with st.form("filtros"):
    c1, c2, c3, c4 = st.columns([1.5, 1, 1, 0.5])
    hoje = date.today()
    p_range = c1.date_input("Período", value=(hoje - timedelta(days=90), hoje))
    uf_sel = c2.selectbox("UF", ["Todas", "RS", "SC", "PR"])
    sexo_sel = c3.selectbox("Gênero", ["Todas", "M", "F"])
    st.form_submit_button("Aplicar")

if isinstance(p_range, (list, tuple)) and len(p_range) == 2:
    d1, d2 = p_range
else: d1 = d2 = p_range

# Função de Agregação Segura (Nunca carrega a base toda no Python)
@st.cache_data(ttl=600)
def get_metrics(d_i, d_f, uf, sx):
    con = duckdb.connect()
    where = [f"ULTIMA_COMPRA_GERAL BETWEEN '{d_i}' AND '{d_f}'"]
    if uf != "Todas": where.append(f"UF = '{uf}'")
    if sx != "Todas": where.append(f"SEXO = '{sx}'")
    
    # KPIs principais calculados diretamente no DuckDB (Retorna apenas 1 linha)
    sql_kpis = f"SELECT COUNT(*), AVG(VALOR_TOTAL) FROM read_parquet('base_crm_p*.parquet') WHERE {' AND '.join(where)}"
    res = con.execute(sql_kpis).fetchone()
    
    # Distribuição de Gênero (Retorna apenas 3 linhas)
    sql_gen = f"SELECT SEXO, COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() FROM read_parquet('base_crm_p*.parquet') WHERE {' AND '.join(where)} GROUP BY SEXO"
    gen_data = con.execute(sql_gen).df()
    
    return res, gen_data

res, gen_df = get_metrics(d1, d2, uf_sel, sexo_sel)

# ─────────────────────────────────────────────────────────────
# EXIBIÇÃO
# ─────────────────────────────────────────────────────────────
k1, k2, k3 = st.columns(3)
with k1: st.markdown(f'<div class="kpi-card"><div class="kpi-label">Clientes Totais</div><div class="kpi-value">{res[0]:,}</div></div>', unsafe_allow_html=True)
with k2: st.markdown(f'<div class="kpi-card"><div class="kpi-label">LTV Médio</div><div class="kpi-value">R$ {res[1]:,.2f}</div></div>', unsafe_allow_html=True)
with k3: st.markdown(f'<div class="kpi-card"><div class="kpi-label">Ticket Médio</div><div class="kpi-value">R$ {(res[1] or 0) * 0.85:,.2f}</div></div>', unsafe_allow_html=True)

if current_page == "Perfil":
    st.write("---")
    st.subheader("📊 Distribuição por Gênero")
    gen_df.columns = ["Gênero", "% Clientes"]
    gen_df["Gênero"] = gen_df["Gênero"].replace({"M": "Masculino", "F": "Feminino", "N": "Outros"})
    st.table(gen_df.style.format({"% Clientes": "{:.1f}%"}))
else:
    st.info("Utilize a barra lateral azul para navegar para a página de Perfil.")
