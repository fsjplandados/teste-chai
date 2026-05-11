import streamlit as st
import pandas as pd
import duckdb
from datetime import date, timedelta

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÃO E DESIGN SYSTEM PREMIUM (RESTAURADO)
# ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="Dashboard CRM — Farmácias São João", layout="wide", initial_sidebar_state="collapsed")

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
    .kpi-value { font-size: 32px; font-weight: 800; color: var(--text-1); letter-spacing: -0.5px; margin: 4px 0; }
    .kpi-sub { font-size: 11px; color: var(--text-3); margin-top: 4px; }
</style>
""", unsafe_allow_html=True)

st.markdown(f"""
<div class="crm-sidebar">
    <div class="logo-circle"><img src="https://upload.wikimedia.org/wikipedia/commons/4/4b/Logo_Farmacias_Sao_Joao.png"></div>
    <a href="#" class="nav-item active"><svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg></a>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# LOGICA DE DADOS (ALINHADA SNOWFLAKE)
# ─────────────────────────────────────────────────────────────
def get_alinhamento_data(d_i, d_f, uf, cid, reg, sx, lj, can, dig):
    con = duckdb.connect()
    source = "read_parquet('base_crm_p*.parquet')"
    
    # Filtros de Dimensão
    where_dim = []
    if uf != "Todas": where_dim.append(f"UF = '{uf}'")
    if cid != "Todas": where_dim.append(f"CIDADE = '{cid}'")
    if reg != "Todas": where_dim.append(f"REGIAO = '{reg}'")
    if sx != "Todas": where_dim.append(f"SEXO = '{sx}'")
    if lj != "Todas": where_dim.append(f"LOJA = '{lj}'")
    
    dim_str = " AND ".join(where_dim) if where_dim else "1=1"
    
    # Tentar usar DATA_INCLUSAO (se não existir, usa PRIMEIRA_COMPRA como fallback seguro)
    cols = con.execute(f"DESCRIBE SELECT * FROM {source} LIMIT 1").df()['column_name'].tolist()
    col_inc = 'DATA_INCLUSAO' if 'DATA_INCLUSAO' in cols else 'PRIMEIRA_COMPRA'
    col_ult = 'ULTIMA_COMPRA_GERAL' if 'ULTIMA_COMPRA_GERAL' in cols else 'ULTIMA_COMPRA'

    # KPI 1: Clientes Totais (Snapshot até d_f)
    # SQL: SELECT COUNT(*) FROM ... WHERE DATA_INCLUSAO <= d_f
    totais = con.execute(f"""
        SELECT COUNT(*) FROM {source} 
        WHERE {dim_str} 
        AND CAST({col_inc} AS DATE) <= '{d_f}'
    """).fetchone()[0]
    
    # KPI 2: Novos Clientes (Aquisição no Período)
    # SQL: SELECT COUNT(*) FROM ... WHERE DATA_INCLUSAO BETWEEN d_i AND d_f
    novos = con.execute(f"""
        SELECT COUNT(*) FROM {source} 
        WHERE {dim_str} 
        AND CAST({col_inc} AS DATE) >= '{d_i}' 
        AND CAST({col_inc} AS DATE) <= '{d_f}'
    """).fetchone()[0]
    
    # KPI 3: Clientes Ativos 90d (Snapshot na data d_f)
    # SQL: SELECT COUNT(*) FROM ... WHERE ULTIMA_COMPRA >= d_f-90 AND ULTIMA_COMPRA <= d_f
    d_90 = d_f - timedelta(days=90)
    ativos = con.execute(f"""
        SELECT COUNT(*) FROM {source} 
        WHERE {dim_str} 
        AND CAST({col_ult} AS DATE) >= '{d_90}' 
        AND CAST({col_ult} AS DATE) <= '{d_f}'
    """).fetchone()[0]
    
    return totais, ativos, novos

# ─────────────────────────────────────────────────────────────
# INTERFACE COM FILTROS COMPLETOS
# ─────────────────────────────────────────────────────────────
st.markdown(f'<h1 style="font-size:24px; font-weight:800; color:#111827; margin-bottom:20px;">Alinhamento CRM — Clientes</h1>', unsafe_allow_html=True)

with st.form("filtros_globais"):
    r1_c1, r1_c2, r1_c3, r1_c4, r1_c5 = st.columns([1.5, 0.8, 0.8, 0.8, 0.8])
    p_range = r1_c1.date_input("Período de Referência", value=(date(2026, 1, 1), date(2026, 1, 31)))
    uf_sel = r1_c2.selectbox("UF", ["Todas", "RS", "SC", "PR"])
    
    # Cidades dinâmicas
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

# Lógica para tratar o intervalo selecionado
if isinstance(p_range, (list, tuple)) and len(p_range) == 2:
    d_i, d_f = p_range
else:
    d_i = d_f = p_range if not isinstance(p_range, (list, tuple)) else p_range[0]

try:
    totais, ativos, novos = get_alinhamento_data(d_i, d_f, uf_sel, cid_sel, reg_sel, sexo_sel, loja_sel, canal_sel, digital_sel)
    
    def card(label, val, sub, icon_svg, color):
        st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-icon" style="background:var(--{color})">{icon_svg}</div>
                <div class="kpi-label">{label}</div>
                <div class="kpi-value">{val}</div>
                <div class="kpi-sub">{sub}</div>
            </div>
        """, unsafe_allow_html=True)

    i_u = '<svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/></svg>'
    i_n = '<svg viewBox="0 0 24 24"><path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><line x1="19" y1="8" x2="19" y2="14"/><line x1="16" y1="11" x2="22" y2="11"/></svg>'
    
    c1, c2, c3 = st.columns(3)
    with c1:
        card("Clientes Totais", f"{totais:,}", "Tamanho da base histórica", i_u, "text-3")
    with c2:
        card("Novos Clientes", f"{novos:,}", f"Período: {d_i.strftime('%d/%m')} a {d_f.strftime('%d/%m')}", i_n, "sky")
    with c3:
        card("Clientes Ativos (90d)", f"{ativos:,}", f"Atividade até {d_f.strftime('%d/%m/%y')}", i_u, "green")

except Exception as e:
    st.error(f"Erro: {e}")
