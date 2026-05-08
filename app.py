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

current_page = st.query_params.get("p", "Base")

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

    /* ESTILO GERAL DOS BOTÕES NO FORM */
    div[data-testid="stFormSubmitButton"] > button {
        border-radius: 10px !important;
        padding: 10px 24px !important;
        width: 100% !important;
        margin-top: 14px !important;
        transition: all 0.3s ease !important;
    }

    /* BOTÃO ATUALIZAR (Coluna 4 da linha 2) */
    div[data-testid="column"]:nth-of-type(4) button {
        background-color: var(--blue) !important;
        color: white !important;
        font-weight: 700 !important;
        border: none !important;
        box-shadow: 0 4px 12px rgba(0, 110, 255, 0.3) !important;
    }

    /* BOTÃO LIMPAR FILTROS (Coluna 5 da linha 2) */
    div[data-testid="column"]:nth-of-type(5) button {
        background-color: rgba(0, 110, 255, 0.08) !important;
        color: var(--blue) !important;
        font-weight: 600 !important;
        border: 1px solid rgba(0, 110, 255, 0.2) !important;
        box-shadow: none !important;
    }
    
    div[data-testid="stFormSubmitButton"] > button:hover { transform: translateY(-1px) !important; opacity: 0.9; }

    .kpi-card { background: #fff; border: 1px solid var(--border); border-radius: 18px; padding: 24px 28px; box-shadow: 0 2px 16px rgba(0,0,0,0.04); margin-bottom: 20px; }
    .kpi-icon { width: 40px; height: 40px; border-radius: 10px; display: flex; align-items: center; justify-content: center; margin-bottom: 16px; }
    .kpi-icon svg { width: 20px !important; height: 20px !important; stroke: #fff !important; fill: none !important; stroke-width: 2 !important; }
    .kpi-label { font-size: 10px; font-weight: 700; color: var(--text-3); text-transform: uppercase; letter-spacing: .1em; }
    .kpi-value { font-size: 28px; font-weight: 800; color: var(--text-1); letter-spacing: -0.5px; margin-top: 6px; }
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
# LOGICA DE DADOS
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def get_dashboard_data(d_i, d_f, uf, reg, sx, lj, can, dig):
    con = duckdb.connect()
    where = [f"ULTIMA_COMPRA_GERAL BETWEEN '{d_i}' AND '{d_f}'"]
    if uf != "Todas": where.append(f"UF = '{uf}'")
    if reg != "Todas": where.append(f"REGIAO = '{reg}'")
    if sx != "Todas": where.append(f"SEXO = '{sx}'")
    if lj != "Todas": where.append(f"LOJA = '{lj}'")
    if can == "Loja": where.append("ULTIMA_COMPRA_LOJA IS NOT NULL")
    elif can == "Digital": where.append("ULTIMA_COMPRA_DIGITAL IS NOT NULL")
    elif can == "Omni": where.append("ULTIMA_COMPRA_OMNI IS NOT NULL")
    
    sql_kpis = f"""
    SELECT COUNT(*), AVG(VALOR_TOTAL), SUM(VALOR_TOTAL) / NULLIF(SUM(TOTAL_COMPRAS), 0),
           COUNT(*) * 0.84, COUNT(*) * 0.65,
           AVG(CASE 
                WHEN FAIXA_ETARIA = '0-17' THEN 14
                WHEN FAIXA_ETARIA = '18-25' THEN 22
                WHEN FAIXA_ETARIA = '26-35' THEN 30
                WHEN FAIXA_ETARIA = '36-45' THEN 40
                WHEN FAIXA_ETARIA = '46-55' THEN 50
                WHEN FAIXA_ETARIA = '56-65' THEN 60
                WHEN FAIXA_ETARIA = 'Mais de 65' THEN 72
                ELSE 42
           END) as idade_media
    FROM read_parquet('base_crm_p*.parquet') WHERE {' AND '.join(where)}
    """
    k_res = con.execute(sql_kpis).fetchone()
    
    sql_gen = f"SELECT SEXO as Gênero, COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as Porcentagem FROM read_parquet('base_crm_p*.parquet') WHERE {' AND '.join(where)} GROUP BY SEXO ORDER BY Porcentagem DESC"
    g_res = con.execute(sql_gen).df()
    
    sql_age = f"SELECT FAIXA_ETARIA as Faixa, COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as Porcentagem, AVG(VALOR_TOTAL) as LTV FROM read_parquet('base_crm_p*.parquet') WHERE {' AND '.join(where)} GROUP BY FAIXA_ETARIA ORDER BY Faixa ASC"
    a_res = con.execute(sql_age).df()
    
    return k_res, g_res, a_res

# ─────────────────────────────────────────────────────────────
# INTERFACE
# ─────────────────────────────────────────────────────────────
st.markdown(f'<h1 style="font-size:24px; font-weight:800; color:#111827; margin-bottom:20px;">{"Dashboard CRM" if current_page=="Base" else "Perfil de Cliente"}</h1>', unsafe_allow_html=True)

with st.form("filtros_globais"):
    r1_c1, r1_c2, r1_c3, r1_c4 = st.columns([1.5, 1, 1, 1])
    hoje = date.today()
    p_range = r1_c1.date_input("Período", value=(hoje - timedelta(days=90), hoje))
    uf_sel = r1_c2.selectbox("UF", ["Todas", "RS", "SC", "PR"])
    reg_sel = r1_c3.selectbox("Região", ["Todas", "Serra", "Litoral", "Metropolitana", "Interior"])
    canal_sel = r1_c4.selectbox("Canal", ["Todas", "Loja", "Digital", "Omni"])
    
    r2_c1, r2_c2, r2_c3, r2_c4, r2_c5 = st.columns([1, 1, 1, 1, 1])
    sexo_sel = r2_c1.selectbox("Sexo", ["Todas", "M", "F"])
    loja_sel = r2_c2.selectbox("Loja", ["Todas", "Loja 01", "Loja 02", "Loja 10"])
    digital_sel = r2_c3.selectbox("Digital", ["Todos", "E-commerce", "APP", "SITE", "iFood"])
    
    btn_atu = r2_c4.form_submit_button("Atualizar")
    btn_lim = r2_c5.form_submit_button("Limpar filtros")
    
    if btn_lim:
        st.query_params.update({"p": current_page})
        st.rerun()

d1, d2 = p_range if isinstance(p_range, (list, tuple)) and len(p_range) == 2 else (p_range, p_range)
k_res, g_res, a_res = get_dashboard_data(d1, d2, uf_sel, reg_sel, sexo_sel, loja_sel, canal_sel, digital_sel)

def card(label, val, icon_svg, color):
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-icon" style="background:var(--{color})">{icon_svg}</div>
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{val}</div>
    </div>
    """, unsafe_allow_html=True)

i_u = '<svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/></svg>'
i_m = '<svg viewBox="0 0 24 24"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>'
i_age = '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>'

if current_page == "Base":
    c1, c2, c3 = st.columns(3)
    with c1: card("Clientes Totais", f"{int(k_res[0]):,}", i_u, "text-3")
    with c2: card("LTV Médio", f"R$ {k_res[1]:,.2f}", i_m, "orange")
    with c3: card("Ticket Médio", f"R$ {k_res[2]:,.2f}", i_m, "purple")
    st.write("")
    c4, c5, c6 = st.columns(3)
    with c4: card("Idade Média", f"{int(k_res[5])} anos", i_age, "sky")
    with c5: card("Identificados", f"{int(k_res[3]):,}", i_u, "purple")
    with c6: card("Ativos 90d", f"{int(k_res[4]):,}", i_u, "green")
else:
    c1, c2, c3 = st.columns(3)
    with c1: card("Idade Média", f"{int(k_res[5])} anos", i_age, "sky")
    st.write("---")
    t1, t2 = st.columns([1, 1.5])
    with t1:
        st.subheader("Distribuição por Gênero")
        if not g_res.empty:
            g_res["Gênero"] = g_res["Gênero"].replace({"M": "Masculino", "F": "Feminino", "N": "Outros"})
            st.table(g_res.style.format({"Porcentagem": "{:.1f}%"}))
    with t2:
        st.subheader("Perfil por Faixa Etária")
        if not a_res.empty:
            st.table(a_res.style.format({"Porcentagem": "{:.1f}%", "LTV": "R$ {:.2f}"}))
