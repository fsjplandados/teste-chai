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
@st.cache_data(ttl=600)
def get_alinhamento_data(d_f, uf, cid, reg, sx, lj, can, dig):
    con = duckdb.connect()
    source = "read_parquet('base_crm_p*.parquet')"
    
    # Filtros de Dimensão
    where = []
    if uf != "Todas": where.append(f"UF = '{uf}'")
    if cid != "Todas": where.append(f"CIDADE = '{cid}'")
    if reg != "Todas": where.append(f"REGIAO = '{reg}'")
    if sx != "Todas": where.append(f"SEXO = '{sx}'")
    if lj != "Todas": where.append(f"LOJA = '{lj}'")
    # Nota: Canal e Digital no Datamart_Clientes costumam ser baseados na última compra
    
    where_str = " AND ".join(where) if where else "1=1"
    
    # KPI 1: Clientes Totais
    totais = con.execute(f"SELECT COUNT(*) FROM {source} WHERE {where_str}").fetchone()[0]
    
    # KPI 2: Clientes Ativos 90d
    d_90 = d_f - timedelta(days=90)
    ativos = con.execute(f"""
        SELECT COUNT(*) FROM {source} 
        WHERE {where_str} 
        AND ULTIMA_COMPRA_GERAL >= '{d_90}' 
        AND ULTIMA_COMPRA_GERAL <= '{d_f}'
    """).fetchone()[0]
    
    return totais, ativos

# ─────────────────────────────────────────────────────────────
# INTERFACE COM FILTROS COMPLETOS
# ─────────────────────────────────────────────────────────────
st.markdown(f'<h1 style="font-size:24px; font-weight:800; color:#111827; margin-bottom:20px;">Alinhamento CRM — Clientes</h1>', unsafe_allow_html=True)

with st.form("filtros_globais"):
    r1_c1, r1_c2, r1_c3, r1_c4, r1_c5 = st.columns([1.5, 0.8, 0.8, 0.8, 0.8])
    data_ref = r1_c1.date_input("Data de Referência (Fim)", value=date(2026, 1, 31))
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

try:
    totais, ativos = get_alinhamento_data(data_ref, uf_sel, cid_sel, reg_sel, sexo_sel, loja_sel, canal_sel, digital_sel)
    
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
    
    c1, c2 = st.columns(2)
    with c1:
        card("Clientes Totais", f"{totais:,}", "Contagem total da base (Datamart)", i_u, "text-3")
    with c2:
        card("Clientes Ativos (90d)", f"{ativos:,}", f"Janela: {(data_ref - timedelta(days=90)).strftime('%d/%m')} a {data_ref.strftime('%d/%m/%y')}", i_u, "green")

except Exception as e:
    st.error(f"Erro: {e}")
