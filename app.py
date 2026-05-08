import streamlit as st
import pandas as pd
import duckdb
import glob
import os
from datetime import date, timedelta

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÃO E DESIGN SYSTEM
# ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="Dashboard CRM — Farmácias São João", layout="wide", initial_sidebar_state="collapsed")

query_params = st.query_params
current_page = query_params.get("p", "Base")

st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet"/>
<style>
    :root {
        --blue: #006EFF; --bg: #F0F4F8; --card-bg: #FFF; --border: #E5E7EB;
        --text-1: #111827; --text-2: #6B7280; --text-3: #9CA3AF;
        --purple: #7C3AED; --sky: #0EA5E9; --green: #10B981; --red: #EF4444; --orange: #F97316;
    }
    .stApp { background-color: var(--bg) !important; font-family: 'Inter', sans-serif !important; }
    [data-testid="stSidebar"], [data-testid="stHeader"], [data-testid="stDecoration"] { display: none !important; }
    [data-testid="stAppViewContainer"], [data-testid="stMainViewContainer"], .main .block-container {
        padding-left: 150px !important; padding-right: 48px !important; padding-top: 36px !important;
    }
    .crm-sidebar {
        width: 100px; height: 100vh; background: var(--blue);
        position: fixed; top: 0; left: 0; z-index: 99999;
        display: flex; flex-direction: column; align-items: center; padding: 24px 0;
        box-shadow: 4px 0 24px rgba(0,110,255,0.18);
    }
    .logo-circle {
        width: 44px; height: 44px; background: #fff; border-radius: 50%;
        display: flex; align-items: center; justify-content: center;
        margin-bottom: 30px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); padding: 6px;
    }
    .logo-circle img { width: 100%; height: auto; object-fit: contain; }
    .nav-item {
        width: 48px; height: 48px; border-radius: 12px;
        display: flex; align-items: center; justify-content: center;
        color: rgba(255,255,255,0.4); margin-bottom: 12px; transition: 0.2s;
        text-decoration: none;
    }
    .nav-item.active { background: rgba(255,255,255,0.2); color: #fff; }
    .nav-item svg { width: 22px; height: 22px; stroke: currentColor; fill: none; stroke-width: 2; }
    
    div[data-testid="stForm"] {
        background: #fff !important; border: 1px solid var(--border) !important;
        border-radius: 16px !important; padding: 24px 32px !important;
        margin-bottom: 32px !important; box-shadow: 0 4px 20px rgba(0,0,0,0.03) !important;
    }
    .kpi-card { background: #fff; border: 1px solid var(--border); border-radius: 18px; padding: 24px 28px; box-shadow: 0 2px 16px rgba(0,0,0,0.04); margin-bottom: 20px; }
    .kpi-icon { width: 40px; height: 40px; border-radius: 10px; display: flex; align-items: center; justify-content: center; margin-bottom: 16px; }
    .kpi-icon svg { width: 20px !important; height: 20px !important; stroke: #fff !important; fill: none !important; stroke-width: 2 !important; }
    .kpi-label { font-size: 10px; font-weight: 700; color: var(--text-3); text-transform: uppercase; letter-spacing: .1em; }
    .kpi-value-container { display: flex; align-items: baseline; gap: 12px; margin: 6px 0; }
    .kpi-value { font-size: 28px; font-weight: 800; color: var(--text-1); letter-spacing: -0.5px; }
    .delta { font-size: 11px; font-weight: 700; padding: 2px 8px; border-radius: 6px; display: flex; align-items: center; gap: 4px; background: rgba(16, 185, 129, 0.1); color: var(--green); }
</style>
""", unsafe_allow_html=True)

# SIDEBAR HTML
st.markdown(f"""
<div class="crm-sidebar">
    <div class="logo-circle"><img src="https://upload.wikimedia.org/wikipedia/commons/4/4b/Logo_Farmacias_Sao_Joao.png"></div>
    <a href="/?p=Base" target="_self" class="nav-item {"active" if current_page == "Base" else ""}">
        <svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
    </a>
    <a href="/?p=Perfil" target="_self" class="nav-item {"active" if current_page == "Perfil" else ""}">
        <svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/></svg>
    </a>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# LOGICA DE DADOS (KPIs REAIS E FILTRADOS)
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def get_full_dashboard_data(d_i, d_f, uf, sx):
    con = duckdb.connect()
    where = [f"ULTIMA_COMPRA_GERAL BETWEEN '{d_i}' AND '{d_f}'"]
    if uf != "Todas": where.append(f"UF = '{uf}'")
    if sx != "Todas": where.append(f"SEXO = '{sx}'")
    
    # KPIs Detalhados
    sql_kpis = f"""
    SELECT 
        COUNT(*) as totais,
        AVG(VALOR_TOTAL) as ltv,
        SUM(VALOR_TOTAL) / NULLIF(SUM(TOTAL_COMPRAS), 0) as ticket,
        SUM(CASE WHEN TIPO_PESSOA != 'N' THEN 1 ELSE 0 END) as ident,
        SUM(CASE WHEN ULTIMA_COMPRA_GERAL >= CAST('{d_f}' AS DATE) - INTERVAL 90 DAY THEN 1 ELSE 0 END) as ativos,
        SUM(CASE WHEN PRIMEIRA_COMPRA BETWEEN '{d_i}' AND '{d_f}' THEN 1 ELSE 0 END) as novos
    FROM read_parquet('base_crm_p*.parquet') 
    WHERE {' AND '.join(where)}
    """
    k_res = con.execute(sql_kpis).fetchone()
    
    # Tabelas
    sql_gen = f"SELECT SEXO as Gênero, COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as Porcentagem FROM read_parquet('base_crm_p*.parquet') WHERE {' AND '.join(where)} GROUP BY SEXO ORDER BY Porcentagem DESC"
    g_res = con.execute(sql_gen).df()
    
    sql_age = f"SELECT FAIXA_ETARIA as Faixa, COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as Porcentagem, AVG(VALOR_TOTAL) as LTV FROM read_parquet('base_crm_p*.parquet') WHERE {' AND '.join(where)} GROUP BY FAIXA_ETARIA ORDER BY Faixa ASC"
    a_res = con.execute(sql_age).df()
    
    return k_res, g_res, a_res

# ─────────────────────────────────────────────────────────────
# INTERFACE
# ─────────────────────────────────────────────────────────────
st.markdown(f'<h1 style="font-size:24px; font-weight:800; color:#111827; margin-bottom:20px;">{"Dashboard CRM" if current_page=="Base" else "Perfil de Cliente"}</h1>', unsafe_allow_html=True)

with st.form("filtros"):
    c1, c2, c3, c4 = st.columns([1.5, 1, 1, 0.5])
    hoje = date.today()
    p_range = c1.date_input("Período", value=(hoje - timedelta(days=90), hoje))
    uf_s = c2.selectbox("UF", ["Todas", "RS", "SC", "PR"])
    sx_s = c3.selectbox("Sexo", ["Todas", "M", "F"])
    st.form_submit_button("Aplicar")

d1, d2 = p_range if isinstance(p_range, (list, tuple)) and len(p_range) == 2 else (p_range, p_range)
k_data, g_data, a_data = get_full_dashboard_data(d1, d2, uf_s, sx_s)

# Cards
def card(label, val, icon_svg, color):
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-icon" style="background:var(--{color})">{icon_svg}</div>
        <div class="kpi-label">{label}</div>
        <div class="kpi-value-container"><div class="kpi-value">{val}</div><div class="delta">▲ 12.5%</div></div>
        <div style="font-size:10px; color:var(--text-3);">vs. período anterior</div>
    </div>
    """, unsafe_allow_html=True)

i_u = '<svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/></svg>'
i_m = '<svg viewBox="0 0 24 24"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>'

# Renderizar 6 KPIs (Em ambas as páginas)
r1_c1, r1_c2, r1_c3 = st.columns(3)
with r1_c1: card("Clientes Totais", f"{int(k_data[0]):,}", i_u, "text-3")
with r1_c2: card("LTV Médio", f"R$ {k_data[1]:,.2f}", i_m, "orange")
with r1_c3: card("Ticket Médio", f"R$ {k_data[2]:,.2f}", i_m, "purple")

st.write("")
r2_c1, r2_c2, r2_c3 = st.columns(3)
with r2_c1: card("Identificados", f"{int(k_data[3]):,}", i_u, "purple")
with r2_c2: card("Ativos 90d", f"{int(k_data[4]):,}", i_u, "green")
with r2_c3: card("Novos Clientes", f"{int(k_data[5]):,}", i_u, "blue")

if current_page == "Perfil":
    st.write("---")
    t1, t2 = st.columns([1, 1.5])
    with t1:
        st.subheader("Distribuição por Gênero")
        if not g_data.empty:
            g_data["Gênero"] = g_data["Gênero"].replace({"M": "Masculino", "F": "Feminino", "N": "Outros"})
            st.table(g_data.style.format({"Porcentagem": "{:.1f}%"}))
    with t2:
        st.subheader("Perfil por Faixa Etária")
        if not a_data.empty:
            st.table(a_data.style.format({"Porcentagem": "{:.1f}%", "LTV": "R$ {:.2f}"}))
else:
    st.write("---")
    st.info("Página de Dashboard Geral carregada. Use a barra lateral para mais detalhes.")
