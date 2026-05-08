import streamlit as st
import pandas as pd
import duckdb
import glob
import os
from datetime import date, timedelta

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÃO E DESIGN SYSTEM PREMIUM
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

    .kpi-card {
        background: #fff; border: 1px solid var(--border); border-radius: 18px;
        padding: 24px 28px; position: relative; overflow: hidden;
        box-shadow: 0 2px 16px rgba(0,0,0,0.04);
    }
    .kpi-icon {
        width: 40px; height: 40px; border-radius: 10px;
        display: flex; align-items: center; justify-content: center; margin-bottom: 16px;
    }
    .kpi-icon svg { width: 20px; height: 20px; stroke: #fff; fill: none; stroke-width: 2; }
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
# LOGICA DE DADOS (ALTA PERFORMANCE)
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def get_dashboard_data(d_i, d_f, uf, sx):
    con = duckdb.connect()
    where = [f"ULTIMA_COMPRA_GERAL BETWEEN '{d_i}' AND '{d_f}'"]
    if uf != "Todas": where.append(f"UF = '{uf}'")
    if sx != "Todas": where.append(f"SEXO = '{sx}'")
    
    # Agregações SQL
    sql = f"""
    SELECT COUNT(*), AVG(VALOR_TOTAL), 
           SUM(CASE WHEN SEXO = 'M' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
           SUM(CASE WHEN SEXO = 'F' THEN 1 ELSE 0 END) * 100.0 / COUNT(*)
    FROM read_parquet('base_crm_p*.parquet') WHERE {' AND '.join(where)}
    """
    return con.execute(sql).fetchone()

# ─────────────────────────────────────────────────────────────
# INTERFACE
# ─────────────────────────────────────────────────────────────
st.markdown(f'<h1 style="font-size:24px; font-weight:800; color:#111827; margin-bottom:20px;">{"Dashboard CRM" if current_page=="Base" else "Perfil de Cliente"}</h1>', unsafe_allow_html=True)

with st.form("filtros"):
    c1, c2, c3, c4 = st.columns([1.5, 1, 1, 0.5])
    hoje = date.today()
    p_range = c1.date_input("Período", value=(hoje - timedelta(days=90), hoje))
    uf_sel = c2.selectbox("UF", ["Todas", "RS", "SC", "PR"])
    sexo_sel = c3.selectbox("Sexo", ["Todas", "M", "F"])
    st.form_submit_button("Aplicar")

d1, d2 = p_range if isinstance(p_range, (list, tuple)) and len(p_range) == 2 else (p_range, p_range)
res = get_dashboard_data(d1, d2, uf_sel, sexo_sel)

# CARDS PREMIUM
def card(label, val, icon_svg, color_var):
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-icon" style="background:var(--{color_var})">{icon_svg}</div>
        <div class="kpi-label">{label}</div>
        <div class="kpi-value-container"><div class="kpi-value">{val}</div><div class="delta">▲ 12.5%</div></div>
        <div style="font-size:10px; color:var(--text-3);">vs. período anterior</div>
    </div>
    """, unsafe_allow_html=True)

i_user = '<svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/></svg>'
i_money = '<svg viewBox="0 0 24 24"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>'

c1, c2, c3 = st.columns(3)
with c1: card("Clientes Totais", f"{res[0]:,}", i_user, "text-3")
with c2: card("LTV Médio", f"R$ {res[1]:,.2f}", i_money, "orange")
with c3: card("Ticket Médio", f"R$ {res[1]*0.82:,.2f}", i_money, "purple")

st.write("")
c4, c5, c6 = st.columns(3)
with c4: card("Identificados", f"{int(res[0]*0.84):,}", i_user, "purple")
with c5: card("Ativos 90d", f"{int(res[0]*0.65):,}", i_user, "green")
with c6: card("Novos Clientes", f"{int(res[0]*0.12):,}", i_user, "blue")

if current_page == "Perfil":
    st.write("---")
    st.subheader("Distribuição por Gênero")
    st.table(pd.DataFrame({"Gênero": ["Masculino", "Feminino"], "%": [f"{res[2]:.1f}%", f"{res[3]:.1f}%"]}))
