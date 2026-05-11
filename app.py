import streamlit as st
import pandas as pd
import duckdb
import glob
import os
import plotly.graph_objects as go
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÃO E DESIGN SYSTEM (IGUAL AO PRINT)
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

    button[kind="primaryFormSubmit"] {
        background-color: var(--blue) !important; color: white !important; border-radius: 10px !important;
        padding: 10px 24px !important; font-weight: 700 !important; border: none !important;
        box-shadow: 0 4px 12px rgba(0, 110, 255, 0.3) !important; width: 100% !important; margin-top: 14px !important;
    }

    .kpi-card { background: #fff; border: 1px solid var(--border); border-radius: 18px; padding: 24px 28px; box-shadow: 0 2px 16px rgba(0,0,0,0.04); margin-bottom: 20px; }
    .kpi-icon { width: 40px; height: 40px; border-radius: 10px; display: flex; align-items: center; justify-content: center; margin-bottom: 16px; }
    .kpi-icon svg { width: 20px; height: 20px; stroke: #fff; fill: none; stroke-width: 2; }
    .kpi-label { font-size: 10px; font-weight: 700; color: var(--text-3); text-transform: uppercase; letter-spacing: .1em; }
    .kpi-value { font-size: 28px; font-weight: 800; color: var(--text-1); letter-spacing: -0.5px; margin: 4px 0; }
    
    .indicators { display: flex; gap: 12px; margin-top: 12px; border-top: 1px solid #f3f4f6; padding-top: 12px; }
    .ind-item { display: flex; flex-direction: column; gap: 2px; }
    .ind-label { font-size: 9px; font-weight: 600; color: var(--text-3); text-transform: uppercase; }
    .ind-val { font-size: 11px; font-weight: 700; display: flex; align-items: center; gap: 3px; }
    .pos { color: var(--green); }
    .neg { color: var(--red); }
</style>
""", unsafe_allow_html=True)

st.markdown(f"""
<div class="crm-sidebar">
    <div class="logo-circle"><img src="https://upload.wikimedia.org/wikipedia/commons/4/4b/Logo_Farmacias_Sao_Joao.png"></div>
    <a href="/?p=Base" target="_self" class="nav-item {"active" if current_page == "Base" else ""}">
        <svg viewBox="0 0 24 24"><polyline points="22-12 18-12 15-21 9-3 6-12 2-12"/></svg>
    </a>
    <a href="/?p=Perfil" target="_self" class="nav-item {"active" if current_page == "Perfil" else ""}">
        <svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/></svg>
    </a>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# LOGICA DE DADOS (STABLE + FILTROS COMPLETOS)
# ─────────────────────────────────────────────────────────────
def run_query(con, source, d1, d2, uf, cid, reg, sx, lj, can, dig):
    where = [f"ULTIMA_COMPRA_GERAL BETWEEN '{d1}' AND '{d2}'"]
    if uf != "Todas": where.append(f"UF = '{uf}'")
    if cid != "Todas": where.append(f"CIDADE = '{cid}'")
    if reg != "Todas": where.append(f"REGIAO = '{reg}'")
    if sx != "Todas": where.append(f"SEXO = '{sx}'")
    if lj != "Todas": where.append(f"LOJA = '{lj}'")
    if can != "Todas": where.append(f"CANAL = '{can}'")
    if dig != "Todos": where.append(f"DIGITAL = '{dig}'")
    
    where_str = " AND ".join(where)
    sql = f"SELECT COUNT(*), AVG(VALOR_TOTAL), SUM(VALOR_TOTAL) / NULLIF(SUM(TOTAL_COMPRAS), 0), SUM(VALOR_TOTAL) FROM {source} WHERE {where_str}"
    return con.execute(sql).fetchone()

@st.cache_data(ttl=600)
def get_dashboard_data(d_i, d_f, uf, cid, reg, sx, lj, can, dig):
    con = duckdb.connect()
    source = "read_parquet('base_crm_p*.parquet')"
    
    current = run_query(con, source, d_i, d_f, uf, cid, reg, sx, lj, can, dig)
    
    delta_days = (d_f - d_i).days + 1
    d_i_mom, d_f_mom = d_i - timedelta(days=delta_days), d_i - timedelta(days=1)
    prev_mom_res = run_query(con, source, d_i_mom, d_f_mom, uf, cid, reg, sx, lj, can, dig)
    
    d_i_yoy, d_f_yoy = d_i - relativedelta(years=1), d_f - relativedelta(years=1)
    prev_yoy_res = run_query(con, source, d_i_yoy, d_f_yoy, uf, cid, reg, sx, lj, can, dig)
    
    return {"current": current, "prev_mom": prev_mom_res, "prev_yoy": prev_yoy_res}

# ─────────────────────────────────────────────────────────────
# INTERFACE (EXATAMENTE COMO O PRINT + CIDADE)
# ─────────────────────────────────────────────────────────────
st.markdown(f'<h1 style="font-size:24px; font-weight:800; color:#111827; margin-bottom:20px;">Dashboard CRM</h1>', unsafe_allow_html=True)

with st.form("filtros_globais"):
    r1_c1, r1_c2, r1_c3, r1_c4, r1_c5 = st.columns([1.5, 0.8, 0.8, 0.8, 0.8])
    p_range = r1_c1.date_input("Período", value=(date(2026, 4, 1), date(2026, 4, 30)))
    uf_sel = r1_c2.selectbox("UF", ["Todas", "RS", "SC", "PR"])
    
    # Busca cidades dinâmicas
    con_tmp = duckdb.connect()
    cidades = ["Todas"] + sorted(con_tmp.execute(f"SELECT DISTINCT CIDADE FROM read_parquet('base_crm_p*.parquet') WHERE CIDADE IS NOT NULL {'AND UF = '+chr(39)+uf_sel+chr(39) if uf_sel != 'Todas' else ''}").df()['CIDADE'].tolist())
    cid_sel = r1_c3.selectbox("Cidade", cidades)
    reg_sel = r1_c4.selectbox("Região", ["Todas", "Serra", "Litoral", "Metropolitana", "Interior"])
    canal_sel = r1_c5.selectbox("Canal", ["Todas", "Loja", "Digital", "Omni"])
    
    r2_c1, r2_c2, r2_c3, r2_c4, r2_c5 = st.columns([1, 1, 1, 1, 1])
    sexo_sel = r2_c1.selectbox("Sexo", ["Todas", "M", "F"])
    loja_sel = r2_c2.selectbox("Loja", ["Todas", "Loja 01", "Loja 02"])
    digital_sel = r2_c3.selectbox("Digital", ["Todos", "E-commerce", "APP", "SITE", "iFood"])
    btn_atu = r2_c4.form_submit_button("Atualizar", type="primary")
    btn_lim = r2_c5.form_submit_button("Limpar filtros", type="secondary")

d1, d2 = p_range if isinstance(p_range, (list, tuple)) and len(p_range) == 2 else (p_range, p_range)

try:
    data = get_dashboard_data(d1, d2, uf_sel, cid_sel, reg_sel, sexo_sel, loja_sel, canal_sel, digital_sel)
    
    def get_delta(cur, prev):
        if not prev or prev == 0: return 0
        return ((cur - prev) / prev) * 100

    def card(label, val, icon_svg, color, cur_val, prev_mom, prev_yoy):
        d_mom = get_delta(cur_val, prev_mom); d_yoy = get_delta(cur_val, prev_yoy)
        def fmt_ind(v):
            if v == 0: return "--"
            icon = "▲" if v > 0 else "▼"; cls = "pos" if v > 0 else "neg"
            return f'<span class="{cls}">{icon} {abs(v):.1f}%</span>'
        st.markdown(f"""<div class="kpi-card"><div class="kpi-icon" style="background:var(--{color})">{icon_svg}</div><div class="kpi-label">{label}</div><div class="kpi-value">{val}</div><div class="indicators"><div class="ind-item"><div class="ind-label">Mês Anterior</div><div class="ind-val">{fmt_ind(d_mom)}</div></div><div class="ind-item"><div class="ind-label">Ano Anterior</div><div class="ind-val">{fmt_ind(d_yoy)}</div></div></div></div>""", unsafe_allow_html=True)

    i_u = '<svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/></svg>'
    i_m = '<svg viewBox="0 0 24 24"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>'

    c1, c2, c3 = st.columns(3)
    with c1: card("Clientes Totais", f"{int(data['current'][0]):,}", i_u, "text-3", data['current'][0], data['prev_mom'][0], data['prev_yoy'][0])
    with c2: card("LTV Médio", f"R$ {data['current'][1]:,.2f}", i_m, "orange", data['current'][1], data['prev_mom'][1], data['prev_yoy'][1])
    with c3: card("Ticket Médio", f"R$ {data['current'][2]:,.2f}", i_m, "purple", data['current'][2], data['prev_mom'][2], data['prev_yoy'][2])

except Exception as e:
    st.error(f"Erro: {e}")
