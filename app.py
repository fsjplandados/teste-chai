import streamlit as st
import pandas as pd
from datetime import date, timedelta
import os

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÃO E DESIGN SYSTEM PREMIUM
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
# CONEXÃO INTELIGENTE (LOCAL VS NUVEM)
# ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    # 1. Tenta usar Streamlit Secrets (Nuvem / Produção)
    try:
        if "connections" in st.secrets and "snowflake" in st.secrets.connections:
            import snowflake.connector
            conn = snowflake.connector.connect(**st.secrets["connections"]["snowflake"])
            return conn
    except Exception:
        pass # Ignora se não houver secrets (ex: rodando local)
        
    # 2. Fallback para ODBC Local (SSO do Usuário no Windows)
    try:
        import pyodbc
        return pyodbc.connect('DSN=SNOWFLAKE_FSJ', autocommit=True)
    except Exception as e:
        st.error(f"Falha ao conectar via ODBC local: {e}")
        st.stop()

# Cache de 10 minutos para não onerar o Snowflake e garantir velocidade
@st.cache_data(ttl=600, show_spinner=False)
def run_query(query):
    conn = get_connection()
    try:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Erro SQL: {e}")
        return pd.DataFrame()

# ─────────────────────────────────────────────────────────────
# LOGICA DE DADOS (TRANSAÇÕES NO SNOWFLAKE)
# ─────────────────────────────────────────────────────────────
def get_dashboard_data(d_i, d_f, uf, cid, reg, sx, age, can, dig, lj):
    
    # 1. Filtros Demográficos (DATAMART_CLIENTES)
    where_c = []
    if sx != "Todas": where_c.append(f"c.SEXO = '{sx}'")
    if age != "Todas":
        if age == "0-17": where_c.append("c.IDADE BETWEEN 0 AND 17")
        elif age == "18-24": where_c.append("c.IDADE BETWEEN 18 AND 24")
        elif age == "25-34": where_c.append("c.IDADE BETWEEN 25 AND 34")
        elif age == "35-44": where_c.append("c.IDADE BETWEEN 35 AND 44")
        elif age == "45-54": where_c.append("c.IDADE BETWEEN 45 AND 54")
        elif age == "55-64": where_c.append("c.IDADE BETWEEN 55 AND 64")
        elif age == "65+": where_c.append("c.IDADE >= 65")
    
    # 2. Filtros Transacionais (VW_ANALISE_VENDAS_BASE e VW_LOJAS)
    where_v = []
    if uf != "Todas": where_v.append(f"l.UF_CIDADE = '{uf}'")
    if cid != "Todas": where_v.append(f"l.NOME_CIDADE = '{cid}'")
    if reg != "Todas": where_v.append(f"l.REGIAO_NOME = '{reg}'")
    if lj != "Todas": where_v.append(f"l.LOJA_NOME = '{lj}'")
    
    if can == "Loja":
        where_v.append("v.TIPO_VENDA_DESCRICAO IN ('Venda Caixa', 'Venda Balcão', 'Auto Atendimento', 'Venda Mais')")
    elif can == "Digital":
        if dig != "Todos":
            where_v.append(f"v.TIPO_VENDA_DESCRICAO = '{dig}'")
        else:
            where_v.append("v.TIPO_VENDA_DESCRICAO IN ('E-commerce', 'APP', 'SITE', 'iFood', 'Rappi')")
    elif can == "Omnichannel":
        where_v.append("v.TIPO_VENDA_DESCRICAO IN ('Venda Tele Entrega', 'APP Tele Entrega', 'SITE Tele Entrega', 'Tele Vizinhança', 'Tele Encaminhada Lojas', 'Venda Tele Entrega Central')")

    cond_c = " AND ".join(where_c) if where_c else "1=1"
    cond_v = " AND ".join(where_v) if where_v else "1=1"

    d_90 = (pd.to_datetime(d_f) - pd.Timedelta(days=90)).strftime('%Y-%m-%d')
    d_i_str = pd.to_datetime(d_i).strftime('%Y-%m-%d')
    d_f_str = pd.to_datetime(d_f).strftime('%Y-%m-%d')

    # Query única massiva que calcula os 3 KPIs em paralelo usando a força bruta do Snowflake
    sql = f"""
    SELECT
        -- 1. Clientes Totais (Snapshot até a data final, apenas filtro demográfico)
        (
            SELECT COUNT(*) 
            FROM FSJ_PRD.GOLD.DATAMART_CLIENTES c
            WHERE c.DATA_INCLUSAO <= '{d_f_str}' AND {cond_c}
        ) AS TOTAIS,
        
        -- 2. Novos Clientes (Entraram no período, apenas filtro demográfico)
        (
            SELECT COUNT(*) 
            FROM FSJ_PRD.GOLD.DATAMART_CLIENTES c
            WHERE c.DATA_INCLUSAO BETWEEN '{d_i_str}' AND '{d_f_str}' AND {cond_c}
        ) AS NOVOS,
        
        -- 3. Clientes Ativos 90D (Lógica Transacional Completa do Usuário)
        (
            SELECT COUNT(DISTINCT v.CPF_CNPJ)
            FROM FSJ_PRD.GOLD.VW_ANALISE_VENDAS_BASE v
            INNER JOIN FSJ_PRD.GOLD.DATAMART_CLIENTES c ON v.CPF_CNPJ = c.CPF_CNPJ
            INNER JOIN FSJ_PRD.GOLD.VW_LOJAS l ON v.LOJA_ID = l.LOJA_ID
            WHERE v.INCLUSAO_DATA BETWEEN '{d_90}' AND '{d_f_str}'
              AND {cond_c}
              AND {cond_v}
        ) AS ATIVOS_90D
    """
    
    df_res = run_query(sql)
    if not df_res.empty:
        return df_res.iloc[0]['TOTAIS'], df_res.iloc[0]['ATIVOS_90D'], df_res.iloc[0]['NOVOS']
    return 0, 0, 0

# ─────────────────────────────────────────────────────────────
# INTERFACE
# ─────────────────────────────────────────────────────────────
st.markdown(f'<h1 style="font-size:24px; font-weight:800; color:#111827; margin-bottom:2px;">Alinhamento CRM — Clientes</h1>', unsafe_allow_html=True)
st.markdown(f'<p style="color:var(--text-2); font-size:14px; margin-bottom:24px;">Conexão Live: <b>Snowflake (Camada Gold)</b></p>', unsafe_allow_html=True)

with st.form("filtros_dashboard"):
    r1_c1, r1_c2, r1_c3, r1_c4, r1_c5 = st.columns([1.5, 0.8, 0.8, 0.8, 0.8])
    p_range = r1_c1.date_input("Período de Referência", value=(date(2025, 1, 1), date(2026, 5, 10)))
    uf_sel = r1_c2.selectbox("UF (Loja)", ["Todas", "RS", "SC", "PR", "SP"])
    
    # Listas estáticas simples para evitar query pesada na carga inicial (no mundo real viriam de uma tabela de domínios pequena)
    cid_sel = r1_c3.selectbox("Cidade", ["Todas", "PASSO FUNDO", "CURITIBA", "PORTO ALEGRE", "SANTA MARIA"]) 
    reg_sel = r1_c4.selectbox("Região", ["Todas", "Serra", "Litoral", "Metropolitana", "Interior"])
    canal_sel = r1_c5.selectbox("Canal de Venda", ["Total", "Loja", "Digital", "Omnichannel"])
    
    r2_c1, r2_c2, r2_c3, r2_c4, r2_c5 = st.columns([1, 1, 1, 1, 1])
    sexo_sel = r2_c1.selectbox("Sexo", ["Todas", "M", "F"])
    age_sel = r2_c2.selectbox("Faixa Etária", ["Todas", "0-17", "18-24", "25-34", "35-44", "45-54", "55-64", "65+"])
    
    dig_sel = "Todos"
    if canal_sel == "Digital":
        dig_sel = r2_c3.selectbox("Tipo Digital", ["Todos", "E-commerce", "APP", "SITE", "iFood", "Rappi"])
    else:
        r2_c3.empty()
        
    loja_sel = r2_c4.text_input("Nome da Loja", placeholder="Ex: SAO JOAO 01")
    if not loja_sel: loja_sel = "Todas"
    
    btn_atu = r2_c5.form_submit_button("Consultar Snowflake", type="primary")

if isinstance(p_range, (list, tuple)) and len(p_range) == 2:
    d_i, d_f = p_range
else:
    d_i = d_f = p_range if not isinstance(p_range, (list, tuple)) else p_range[0]

if btn_atu or 'carregado' not in st.session_state:
    st.session_state['carregado'] = True
    with st.spinner("Consultando 1.7 Bilhões de transações no Snowflake..."):
        totais, ativos, novos = get_dashboard_data(d_i, d_f, uf_sel, cid_sel, reg_sel, sexo_sel, age_sel, canal_sel, dig_sel, loja_sel)
        
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
        with c1: card("Clientes Totais", f"{totais:,}", "Tamanho da base", i_u, "text-3")
        with c2: card("Novos Clientes", f"{novos:,}", f"Cadastro no período", i_n, "sky")
        with c3: card("Clientes Ativos (90d)", f"{ativos:,}", f"Compraram nos últimos 90d", i_u, "green")

        st.markdown(f"""
            <div style="margin-top: 40px; padding-top: 20px; border-top: 1px solid var(--border); color: var(--text-3); font-size: 11px; text-align: center;">
                Arquitetura: <b>Live Connection (Snowflake Native)</b> | Filtros aplicados diretamente sobre a VW_ANALISE_VENDAS_BASE e DATAMART_CLIENTES.
            </div>
        """, unsafe_allow_html=True)
