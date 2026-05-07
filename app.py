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

st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet"/>
<style>
    :root {
        --blue: #006EFF; --bg: #F0F4F8; --card-bg: #FFF; --border: #E5E7EB;
        --text-1: #111827; --text-2: #6B7280; --text-3: #9CA3AF;
        --purple: #7C3AED; --sky: #0EA5E9; --green: #10B981; --orange: #F97316;
    }

    /* Base */
    .stApp { background-color: var(--bg) !important; font-family: 'Inter', sans-serif !important; }
    [data-testid="stSidebar"], [data-testid="stHeader"], [data-testid="stDecoration"] { display: none !important; }
    
    /* Layout */
    .main .block-container {
        padding: 24px 48px 24px 110px !important;
        max-width: 100% !important;
    }

    /* ── Sidebar Azul (Original crm.bat) ── */
    .crm-sidebar {
        width: 80px; height: 100vh; background: var(--blue);
        position: fixed; top: 0; left: 0;
        display: flex; flex-direction: column; align-items: center;
        padding: 24px 0; z-index: 9999;
        box-shadow: 4px 0 24px rgba(0,110,255,0.18);
    }
    .logo-circle {
        width: 44px; height: 44px; background: #fff; border-radius: 50%;
        display: flex; align-items: center; justify-content: center;
        margin-bottom: 30px; box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        padding: 6px;
    }
    .logo-circle img { width: 100%; height: auto; object-fit: contain; }
    
    .nav-item {
        width: 48px; height: 48px; border-radius: 12px;
        display: flex; align-items: center; justify-content: center;
        color: rgba(255,255,255,0.4); margin-bottom: 12px;
        transition: 0.2s; cursor: pointer;
    }
    .nav-item.active { background: rgba(255,255,255,0.2); color: #fff; }
    .nav-item svg { width: 22px; height: 22px; stroke: currentColor; fill: none; stroke-width: 2; }

    /* ── Título ── */
    .dash-title { font-size: 24px; font-weight: 800; color: var(--text-1); margin-bottom: 20px; }

    /* ── Barra de Filtros (Estilo Premium) ── */
    .filter-container {
        background: #fff; border: 1px solid var(--border); border-radius: 14px;
        padding: 16px 24px; margin-bottom: 28px;
        box-shadow: 0 2px 12px rgba(0,0,0,0.04);
    }
    .stSelectbox label, .stDateInput label {
        font-size: 10px !important; font-weight: 700 !important;
        color: var(--text-3) !important; text-transform: uppercase !important;
        letter-spacing: .08em !important; margin-bottom: 4px !important;
    }
    div[data-testid="stForm"] { border: none !important; padding: 0 !important; background: transparent !important; box-shadow: none !important; }

    /* ── Grid de Cards (Original crm.bat) ── */
    .kpi-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 24px; }
    .kpi-card {
        background: #fff; border: 1px solid var(--border); border-radius: 18px;
        padding: 24px 28px; position: relative; overflow: hidden;
        box-shadow: 0 2px 16px rgba(0,0,0,0.04); transition: transform 0.2s;
    }
    .kpi-card:hover { transform: translateY(-4px); box-shadow: 0 12px 32px rgba(0,110,255,0.08); }
    .kpi-card::before { content: ''; position: absolute; top: 0; left: 0; width: 100%; height: 4px; }
    
    .kpi-icon {
        width: 42px; height: 42px; border-radius: 10px;
        display: flex; align-items: center; justify-content: center; margin-bottom: 18px;
    }
    .kpi-icon svg { width: 20px; height: 20px; stroke: #fff; fill: none; stroke-width: 2; }
    .kpi-label { font-size: 10px; font-weight: 700; color: var(--text-2); text-transform: uppercase; letter-spacing: .1em; }
    .kpi-value { font-size: 28px; font-weight: 800; color: var(--text-1); margin: 6px 0; letter-spacing: -0.5px; }
    .kpi-desc { font-size: 12px; color: var(--text-3); }

    /* Cores dos Cards */
    .c-gray::before   { background: var(--text-3); }
    .c-purple::before { background: var(--purple); }
    .c-green::before  { background: var(--green); }
    .c-blue::before   { background: var(--sky); }
    .c-orange::before { background: var(--orange); }

    /* Botão Limpar */
    .stButton button {
        border-radius: 8px !important; font-size: 12px !important;
        padding: 4px 16px !important; margin-top: 24px !important;
    }
</style>

<div class="crm-sidebar">
    <div class="logo-circle">
        <img src="https://upload.wikimedia.org/wikipedia/commons/4/4b/Logo_Farmacias_Sao_Joao.png">
    </div>
    <div class="nav-item"><svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg></div>
    <div class="nav-item active"><svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg></div>
    <div class="nav-item"><svg viewBox="0 0 24 24"><path d="M21.21 15.89A10 10 0 1 1 8 2.83"/><path d="M22 12A10 10 0 0 0 12 2v10z"/></svg></div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# DADOS (DUCKDB)
# ─────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_FILES = sorted(glob.glob(os.path.join(BASE_DIR, "base_crm_p*.parquet")))
PARQUET_FILES = [f.replace("\\", "/") for f in PARQUET_FILES]

@st.cache_resource
def get_con(): return duckdb.connect(database=":memory:")
con = get_con()

@st.cache_data(ttl="1d")
def carregar_lookup(files: tuple):
    return con.execute("SELECT DISTINCT UF, CIDADE, LOJA, REGIAO, SEXO, TIPO_PESSOA FROM read_parquet(?) WHERE UF IS NOT NULL", [list(files)]).df()

lkp = carregar_lookup(tuple(PARQUET_FILES))

def opts(col, by=None, val=None):
    base = lkp if (not by or val == "Todas") else lkp[lkp[by] == val]
    return ["Todas"] + sorted(base[col].dropna().unique().tolist())

# ─────────────────────────────────────────────────────────────
# INTERFACE
# ─────────────────────────────────────────────────────────────
st.markdown('<div class="dash-title">Dashboard CRM</div>', unsafe_allow_html=True)

with st.container():
    st.markdown('<div class="filter-container">', unsafe_allow_html=True)
    with st.form("filtros_form", clear_on_submit=False):
        c1, c2, c3, c4, c5 = st.columns([1.1, 1.1, 0.9, 0.6, 1.4])
        dt_ini = c1.date_input("Data Início", value=date(2025, 1, 1))
        dt_fim = c2.date_input("Data Término", value=date.today())
        canal  = c3.selectbox("Canal", ["Total", "Loja", "Digital", "Omnichannel"])
        uf_sel = c4.selectbox("UF", opts("UF"))
        cid_sel = c5.selectbox("Cidade", opts("CIDADE", "UF", uf_sel))

        c6, c7, c8, c9, c10, c11 = st.columns([1.4, 0.8, 1, 0.8, 0.8, 0.5])
        loj_sel = c6.selectbox("Loja", opts("LOJA", "CIDADE", cid_sel) if cid_sel != "Todas" else opts("LOJA", "UF", uf_sel))
        reg_sel = c7.selectbox("Região", opts("REGIAO"))
        faixa_sel = c8.selectbox("Faixa Etária", ["Todas", "Menor de 24", "Entre 25 e 34", "Entre 35 e 44", "Entre 45 e 54", "Entre 55 e 64", "Mais de 65"])
        sexo_sel = c9.selectbox("Sexo", opts("SEXO"))
        tipo_sel = c10.selectbox("Tipo Cliente", opts("TIPO_PESSOA"))
        c11.form_submit_button("Filtrar")
    st.markdown('</div>', unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# CÁLCULOS (ESTÁVEL E DINÂMICO)
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def calcular(files, uf, cid, loj, reg, faixa, sexo, tipo, canal, dt_i, dt_f):
    col_u = {"Loja": "ULTIMA_COMPRA_LOJA", "Digital": "ULTIMA_COMPRA_DIGITAL", "Omnichannel": "ULTIMA_COMPRA_OMNI"}.get(canal, "ULTIMA_COMPRA_GERAL")
    
    # Filtros base
    conds, params = ["1=1"], [list(files)]
    
    # Filtro de Data na Base (Total de clientes ativos no período)
    conds.append(f"{col_u} BETWEEN ? AND ?")
    params.append(dt_i.strftime("%Y-%m-%d"))
    params.append(dt_f.strftime("%Y-%m-%d"))

    def add(col, v):
        if v != "Todas": conds.append(f"{col} = ?"); params.append(v)
    
    add("UF", uf); add("CIDADE", cid); add("LOJA", loj)
    add("REGIAO", reg); add("FAIXA_ETARIA", faixa); add("SEXO", sexo); add("TIPO_PESSOA", tipo)
    
    # Ativos calculados retroativamente a partir da data final selecionada
    lim_ativos = dt_f - timedelta(days=90)
    
    sql = f"""
    SELECT COUNT(*), COUNT(PRIMEIRA_COMPRA),
           SUM(CASE WHEN PRIMEIRA_COMPRA BETWEEN '{dt_i}' AND '{dt_f}' THEN 1 ELSE 0 END),
           SUM(CASE WHEN {col_u} BETWEEN '{lim_ativos}' AND '{dt_f}' THEN 1 ELSE 0 END),
           AVG(VALOR_TOTAL), SUM(VALOR_TOTAL) / NULLIF(SUM(TOTAL_COMPRAS), 0)
    FROM read_parquet(?) WHERE {" AND ".join(conds)}
    """
    return con.execute(sql, params).fetchone()

try:
    total, ident, novos, ativos, ltv, ticket = calcular(tuple(PARQUET_FILES), uf_sel, cid_sel, loj_sel, reg_sel, faixa_sel, sexo_sel, tipo_sel, canal, dt_ini, dt_fim)
except Exception as e:
    st.error(f"Erro ao filtrar: {e}")
    st.stop()

# ─────────────────────────────────────────────────────────────
# EXIBIÇÃO
# ─────────────────────────────────────────────────────────────
def fmt_n(v): return f"{int(v or 0):,}".replace(",", ".")
def fmt_br(v): return "R$ " + f"{float(v or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

st.markdown(f"""
<div class="kpi-grid">
  <div class="kpi-card c-gray">
    <div class="kpi-icon" style="background:var(--text-3)"><svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg></div>
    <div class="kpi-label">Clientes Totais</div>
    <div class="kpi-value">{fmt_n(total)}</div>
    <div class="kpi-desc">Total na seleção</div>
  </div>
  <div class="kpi-card c-purple">
    <div class="kpi-icon" style="background:var(--purple)"><svg viewBox="0 0 24 24"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg></div>
    <div class="kpi-label">Identificados</div>
    <div class="kpi-value">{fmt_n(ident)}</div>
    <div class="kpi-desc">Com histórico</div>
  </div>
  <div class="kpi-card c-green">
    <div class="kpi-icon" style="background:var(--green)"><svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg></div>
    <div class="kpi-label">Ativos (90 dias)</div>
    <div class="kpi-value">{fmt_n(ativos)}</div>
    <div class="kpi-desc">No canal {canal}</div>
  </div>
  <div class="kpi-card c-blue">
    <div class="kpi-icon" style="background:var(--sky)"><svg viewBox="0 0 24 24"><path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="8.5" cy="7" r="4"/><line x1="20" y1="8" x2="20" y2="14"/><line x1="23" y1="11" x2="17" y2="11"/></svg></div>
    <div class="kpi-label">Novos Clientes</div>
    <div class="kpi-value">{fmt_n(novos)}</div>
    <div class="kpi-desc">1ª compra no período</div>
  </div>
  <div class="kpi-card c-orange">
    <div class="kpi-icon" style="background:var(--orange)"><svg viewBox="0 0 24 24"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg></div>
    <div class="kpi-label">LTV Médio</div>
    <div class="kpi-value">{fmt_br(ltv)}</div>
    <div class="kpi-desc">Receita por cliente</div>
  </div>
  <div class="kpi-card c-purple">
    <div class="kpi-icon" style="background:var(--purple)"><svg viewBox="0 0 24 24"><rect x="2" y="5" width="20" height="14" rx="2"/><line x1="2" y1="10" x2="22" y2="10"/></svg></div>
    <div class="kpi-label">Ticket Médio</div>
    <div class="kpi-value">{fmt_br(ticket)}</div>
    <div class="kpi-desc">Por transação</div>
  </div>
</div>
""", unsafe_allow_html=True)
