import streamlit as st
import pandas as pd
import duckdb
import glob
import os
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

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
        --purple: #7C3AED; --sky: #0EA5E9; --green: #10B981; --red: #EF4444; --orange: #F97316;
    }

    .stApp { background-color: var(--bg) !important; font-family: 'Inter', sans-serif !important; }
    [data-testid="stSidebar"], [data-testid="stHeader"], [data-testid="stDecoration"] { display: none !important; }
    
    [data-testid="stAppViewContainer"], [data-testid="stMainViewContainer"], .main .block-container {
        padding-left: 150px !important;
        padding-right: 48px !important;
        padding-top: 36px !important;
    }

    .crm-sidebar {
        width: 100px; height: 100vh; background: var(--blue);
        position: fixed; top: 0; left: 0;
        display: flex; flex-direction: column; align-items: center;
        padding: 24px 0; z-index: 99999;
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
        box-shadow: 0 2px 16px rgba(0,0,0,0.04); margin-bottom: 24px;
    }
    .kpi-label { font-size: 10px; font-weight: 700; color: var(--text-3); text-transform: uppercase; letter-spacing: .1em; }
    .kpi-value { font-size: 28px; font-weight: 800; color: var(--text-1); margin: 6px 0; }
</style>
""", unsafe_allow_html=True)

# SIDEBAR
st.markdown(f"""
<div class="crm-sidebar">
    <div class="logo-circle"><img src="https://upload.wikimedia.org/wikipedia/commons/4/4b/Logo_Farmacias_Sao_Joao.png"></div>
    <a href="/?p=Base" target="_self" class="nav-item {"active" if current_page == "Base" else ""}">
        <svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
    </a>
    <a href="/?p=Perfil" target="_self" class="nav-item {"active" if current_page == "Perfil" else ""}">
        <svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
    </a>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# DADOS (DUCKDB)
# ─────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_FILES = sorted(glob.glob(os.path.join(BASE_DIR, "base_crm_p*.parquet")))

@st.cache_data(ttl=3600)
def load_data(d_ini, d_fim, uf="Todas", sexo="Todas"):
    if not PARQUET_FILES: return pd.DataFrame()
    con = duckdb.connect()
    where = ["ULTIMA_COMPRA_GERAL BETWEEN ? AND ?"]
    params = [d_ini.strftime("%Y-%m-%d"), d_fim.strftime("%Y-%m-%d")]
    if uf != "Todas": where.append("UF = ?"); params.append(uf)
    if sexo != "Todas": where.append("SEXO = ?"); params.append(sexo)
    
    sql = f"SELECT * FROM read_parquet('base_crm_p*.parquet') WHERE {' AND '.join(where)}"
    return con.execute(sql, params).df()

# ─────────────────────────────────────────────────────────────
# INTERFACE
# ─────────────────────────────────────────────────────────────
st.markdown(f'<div style="font-size:24px; font-weight:800; color:#111827; margin-bottom:20px;">{"Dashboard CRM" if current_page == "Base" else "Perfil de Cliente"}</div>', unsafe_allow_html=True)

with st.form("filtros"):
    c1, c2, c3, c4 = st.columns([1.5, 1, 1, 0.5])
    hoje = date.today()
    p_range = c1.date_input("Período", value=(hoje - timedelta(days=90), hoje))
    uf_sel = c2.selectbox("UF", ["Todas", "RS", "SC", "PR"])
    sexo_sel = c3.selectbox("Sexo", ["Todas", "M", "F", "N"])
    btn = c4.form_submit_button("Aplicar")

if isinstance(p_range, (list, tuple)) and len(p_range) == 2:
    d1, d2 = p_range
else: d1 = d2 = p_range

df = load_data(d1, d2, uf=uf_sel, sexo=sexo_sel)

if df.empty:
    st.warning("⚠️ Aguardando dados ou nenhum registro encontrado para o período.")
else:
    # Renderização de KPIs (Dados reais agora!)
    c1, c2, c3 = st.columns(3)
    def kpi(label, val, icon_bg):
        st.markdown(f"""<div class="kpi-card"><div class="kpi-label">{label}</div><div class="kpi-value">{val}</div></div>""", unsafe_allow_html=True)

    with c1: kpi("Clientes Totais", f"{len(df):,}".replace(",", "."), "var(--text-3)")
    with c2: kpi("LTV Médio", f"R$ {df['VALOR_TOTAL'].mean():,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), "var(--orange)")
    with c3: kpi("Ticket Médio", f"R$ {df['VALOR_TOTAL'].mean():,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), "var(--purple)")

    if current_page == "Perfil":
        st.write("---")
        st.subheader("📊 Distribuição por Gênero (Snowflake Data)")
        
        # Cálculo de % por Gênero
        genero_df = df['SEXO'].value_counts(normalize=True).reset_index()
        genero_df.columns = ['Gênero', '% Clientes']
        genero_df['% Clientes'] = (genero_df['% Clientes'] * 100).map('{:.1f}%'.format)
        
        # Tradução amigável
        genero_df['Gênero'] = genero_df['Gênero'].replace({'M': 'Masculino', 'F': 'Feminino', 'N': 'Não Identificado'})
        
        st.table(genero_df)
    else:
        st.write("---")
        st.dataframe(df.head(100), use_container_width=True)
