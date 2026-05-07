import streamlit as st
import pandas as pd
import duckdb
import glob
import os
from datetime import date, timedelta

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÃO E CSS
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
        --blue:#006EFF; --bg:#F0F4F8; --card-bg:#FFF; --border:#E5E7EB;
        --text-1:#111827; --text-2:#6B7280; --text-3:#9CA3AF;
        --purple:#7C3AED; --sky:#0EA5E9; --green:#10B981; --orange:#F97316;
    }

    /* Forçar o fundo e fonte em tudo */
    .stApp { background-color: var(--bg) !important; font-family: 'Inter', sans-serif !important; }
    
    /* Esconder elementos nativos desnecessários */
    [data-testid="stSidebar"], [data-testid="stHeader"] { display: none !important; }
    
    /* Ajustar o container principal para dar espaço à sidebar azul */
    .main .block-container {
        padding-left: 120px !important;
        padding-right: 40px !important;
        padding-top: 40px !important;
        max-width: 100% !important;
    }

    /* ── Sidebar Azul Fixa ── */
    .custom-sidebar {
        width: 80px; height: 100vh; background: var(--blue);
        position: fixed; top: 0; left: 0;
        display: flex; flex-direction: column; align-items: center;
        padding: 24px 0; z-index: 9999;
        box-shadow: 4px 0 24px rgba(0,110,255,0.15);
    }
    .logo-img { width: 40px !important; height: auto; margin-bottom: 40px; }
    .nav-item {
        width: 48px; height: 48px; border-radius: 12px;
        display: flex; align-items: center; justify-content: center;
        color: rgba(255,255,255,0.5); margin-bottom: 16px;
        transition: 0.2s; cursor: pointer;
    }
    .nav-item.active { background: rgba(255,255,255,0.2); color: #fff; }
    .nav-item svg { width: 22px; height: 22px; stroke: currentColor; fill: none; stroke-width: 2; }

    /* ── Barra de Filtros (Estilo do Container) ── */
    [data-testid="stVerticalBlock"] > div:has(div.filter-box) {
        background: #fff !important;
        border: 1px solid var(--border) !important;
        border-radius: 14px !important;
        padding: 24px !important;
        margin-bottom: 30px !important;
        box-shadow: 0 2px 12px rgba(0,0,0,0.04) !important;
    }
    
    /* Labels dos Filtros */
    .stSelectbox label, .stDateInput label {
        font-size: 11px !important; font-weight: 700 !important;
        color: var(--text-2) !important; text-transform: uppercase !important;
        letter-spacing: .05em !important; margin-bottom: 6px !important;
    }

    /* ── Cards de Métricas ── */
    .grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 24px; }
    .card {
        background: #fff; border: 1px solid var(--border); border-radius: 18px;
        padding: 28px; position: relative; overflow: hidden;
        box-shadow: 0 2px 16px rgba(0,0,0,0.04);
    }
    .card::before { content: ''; position: absolute; top: 0; left: 0; width: 100%; height: 4px; }
    .card.c-gray::before   { background: var(--text-3); }
    .card.c-purple::before { background: var(--purple); }
    .card.c-green::before  { background: var(--green); }
    .card.c-blue::before   { background: var(--sky); }
    .card.c-orange::before { background: var(--orange); }

    .card-icon {
        width: 44px; height: 44px; border-radius: 12px;
        display: flex; align-items: center; justify-content: center; margin-bottom: 18px;
    }
    .card-icon svg { width: 22px; height: 22px; stroke: #fff; fill: none; stroke-width: 2; }
    .card-label { font-size: 11px; font-weight: 700; color: var(--text-3); text-transform: uppercase; letter-spacing: .1em; }
    .card-value { font-size: 32px; font-weight: 800; color: var(--text-1); margin: 8px 0; }
    .card-desc  { font-size: 12px; color: var(--text-3); }
    
    /* Título */
    .dash-title { font-size: 26px; font-weight: 800; color: var(--text-1); margin-bottom: 24px; }
</style>

<div class="custom-sidebar">
    <img src="https://upload.wikimedia.org/wikipedia/commons/4/4b/Logo_Farmacias_Sao_Joao.png" class="logo-img">
    <div class="nav-item"><svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg></div>
    <div class="nav-item active"><svg viewBox="0 0 24 24"><circle cx="9" cy="7" r="4"/><path d="M3 21v-2a4 4 0 0 1 4-4h4a4 4 0 0 1 4 4v2"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/><path d="M21 21v-2a4 4 0 0 0-3-3.87"/></svg></div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# LÓGICA DE DADOS (DUCKDB)
# ─────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_FILES = sorted(glob.glob(os.path.join(BASE_DIR, "base_crm_p*.parquet")))
PARQUET_FILES = [f.replace("\\", "/") for f in PARQUET_FILES]

@st.cache_resource
def get_con(_files): return duckdb.connect(database=":memory:")
con = get_con(tuple(PARQUET_FILES))

@st.cache_data(ttl="1d")
def carregar_lookup(files: tuple):
    return con.execute("SELECT DISTINCT UF, CIDADE, LOJA, REGIAO, SEXO, TIPO_PESSOA FROM read_parquet(?) WHERE UF IS NOT NULL", [list(files)]).df()

lkp = carregar_lookup(tuple(PARQUET_FILES))

def opts(col, filter_col=None, filter_val=None):
    base = lkp
    if filter_col and filter_val and filter_val != "Todas":
        base = base[base[filter_col] == filter_val]
    return ["Todas"] + sorted(base[col].dropna().unique().tolist())

# ─────────────────────────────────────────────────────────────
# UI - TÍTULO E FILTROS
# ─────────────────────────────────────────────────────────────
st.markdown('<div class="dash-title">Dashboard CRM</div>', unsafe_allow_html=True)

# Container de Filtros
with st.container():
    st.markdown('<div class="filter-box"></div>', unsafe_allow_html=True)
    
    # Linha 1
    c1, c2, c3, c4, c5 = st.columns([1.2, 1.2, 1, 0.8, 1.5])
    dt_ini = c1.date_input("Data Início", value=date(2025, 1, 1))
    dt_fim = c2.date_input("Data Término", value=date.today())
    canal = c3.selectbox("Canal", ["Total", "Loja", "Digital", "Omnichannel"])
    uf_sel = c4.selectbox("UF", opts("UF"))
    cid_sel = c5.selectbox("Cidade", opts("CIDADE", "UF", uf_sel))
    
    # Linha 2
    c6, c7, c8, c9, c10 = st.columns([1.5, 1, 1, 1, 0.5])
    loj_sel = c6.selectbox("Loja", opts("LOJA", "CIDADE", cid_sel) if cid_sel != "Todas" else opts("LOJA", "UF", uf_sel))
    reg_sel = c7.selectbox("Região", opts("REGIAO"))
    faixa_sel = c8.selectbox("Faixa Etária", ["Todas", "Menor de 24", "Entre 25 e 34", "Entre 35 e 44", "Entre 45 e 54", "Entre 55 e 64", "Mais de 65"])
    sexo_sel = c9.selectbox("Sexo", opts("SEXO"))
    if c10.button("Limpar"): st.rerun()

# ─────────────────────────────────────────────────────────────
# CÁLCULOS
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def calcular(files, uf, cid, loj, reg, faixa, sexo, canal, dt_i, dt_f):
    col_u = {"Loja": "ULTIMA_COMPRA_LOJA", "Digital": "ULTIMA_COMPRA_DIGITAL", "Omnichannel": "ULTIMA_COMPRA_OMNI"}.get(canal, "ULTIMA_COMPRA_GERAL")
    conds, params = ["1=1"], [list(files)]
    def add(col, val):
        if val != "Todas": conds.append(f"{col} = ?"); params.append(val)
    add("UF", uf); add("CIDADE", cid); add("LOJA", loj); add("REGIAO", reg); add("FAIXA_ETARIA", faixa); add("SEXO", sexo)
    
    sql = f"""
    SELECT COUNT(*), COUNT(PRIMEIRA_COMPRA),
           SUM(CASE WHEN PRIMEIRA_COMPRA BETWEEN '{dt_i}' AND '{dt_f}' THEN 1 ELSE 0 END),
           SUM(CASE WHEN {col_u} BETWEEN '{dt_f - timedelta(days=90)}' AND '{dt_f}' THEN 1 ELSE 0 END),
           AVG(VALOR_TOTAL), SUM(VALOR_TOTAL) / NULLIF(SUM(TOTAL_COMPRAS), 0)
    FROM read_parquet(?) WHERE {" AND ".join(conds)}
    """
    return con.execute(sql, params).fetchone()

total, ident, novos, ativos, ltv, ticket = calcular(tuple(PARQUET_FILES), uf_sel, cid_sel, loj_sel, reg_sel, faixa_sel, sexo_sel, canal, dt_ini, dt_fim)

# ─────────────────────────────────────────────────────────────
# CARDS DE MÉTRICAS
# ─────────────────────────────────────────────────────────────
def fmt_n(v): return f"{int(v or 0):,}".replace(",", ".")
def fmt_br(v): return "R$ " + f"{float(v or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

st.markdown(f"""
<div class="grid">
  <div class="card c-gray">
    <div class="card-icon" style="background:var(--text-3)"><svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg></div>
    <div class="card-label">Clientes Totais</div>
    <div class="card-value">{fmt_n(total)}</div>
    <div class="card-desc">Total na seleção</div>
  </div>
  <div class="card c-purple">
    <div class="card-icon" style="background:var(--purple)"><svg viewBox="0 0 24 24"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg></div>
    <div class="card-label">Identificados</div>
    <div class="card-value">{fmt_n(ident)}</div>
    <div class="card-desc">Com histórico</div>
  </div>
  <div class="card c-green">
    <div class="card-icon" style="background:var(--green)"><svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg></div>
    <div class="card-label">Ativos (90 dias)</div>
    <div class="card-value">{fmt_n(ativos)}</div>
    <div class="card-desc">Canal {canal}</div>
  </div>
  <div class="card c-blue">
    <div class="card-icon" style="background:var(--sky)"><svg viewBox="0 0 24 24"><path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="8.5" cy="7" r="4"/><line x1="20" y1="8" x2="20" y2="14"/><line x1="23" y1="11" x2="17" y2="11"/></svg></div>
    <div class="card-label">Novos Clientes</div>
    <div class="card-value">{fmt_n(novos)}</div>
    <div class="card-desc">Primeira compra</div>
  </div>
  <div class="card c-orange">
    <div class="card-icon" style="background:var(--orange)"><svg viewBox="0 0 24 24"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg></div>
    <div class="card-label">LTV Médio</div>
    <div class="card-value">{fmt_br(ltv)}</div>
    <div class="card-desc">Receita por cliente</div>
  </div>
  <div class="card c-purple">
    <div class="card-icon" style="background:var(--purple)"><svg viewBox="0 0 24 24"><rect x="2" y="5" width="20" height="14" rx="2"/><line x1="2" y1="10" x2="22" y2="10"/></svg></div>
    <div class="card-label">Ticket Médio</div>
    <div class="card-value">{fmt_br(ticket)}</div>
    <div class="card-desc">Valor por transação</div>
  </div>
</div>
""", unsafe_allow_html=True)
