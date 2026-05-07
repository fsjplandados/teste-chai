import streamlit as st
import pandas as pd
import duckdb
import glob
import os
from datetime import date, timedelta

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÃO
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard CRM — Farmácias São João",
    layout="wide",
    initial_sidebar_state="collapsed", # Esconde a lateral padrão do Streamlit
)

# ─────────────────────────────────────────────────────────────
# DESIGN SYSTEM COMPLETO (Baseado no seu crm.html)
# ─────────────────────────────────────────────────────────────
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet"/>
<style>
  :root {
    --blue:#006EFF; --bg:#F0F4F8; --card-bg:#FFF; --border:#E5E7EB;
    --text-1:#111827; --text-2:#6B7280; --text-3:#9CA3AF;
    --purple:#7C3AED; --sky:#0EA5E9; --green:#10B981; --orange:#F97316;
    --sidebar-w: 80px;
  }
  
  /* Reset Geral */
  html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; background-color: var(--bg); }
  [data-testid="stSidebar"] { display: none; } /* Esconde sidebar nativa */
  [data-testid="stHeader"] { background: rgba(0,0,0,0); }

  /* ── Sidebar Azul (Original) ── */
  .crm-sidebar {
    width: var(--sidebar-w);
    height: 100vh;
    background: var(--blue);
    position: fixed;
    top: 0; left: 0;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 24px 0;
    box-shadow: 4px 0 24px rgba(0,110,255,0.18);
    z-index: 9999;
  }
  .logo-img { width: 45px; height: auto; margin-bottom: 40px; }
  .nav-item {
    width: 48px; height: 48px; border-radius: 12px;
    display: flex; align-items: center; justify-content: center;
    color: rgba(255,255,255,0.5); margin-bottom: 12px;
    transition: 0.2s; cursor: pointer;
  }
  .nav-item:hover { background: rgba(255,255,255,0.15); color: #fff; }
  .nav-item.active { background: rgba(255,255,255,0.22); color: #fff; box-shadow: 0 0 0 2px rgba(255,255,255,0.3); }
  .nav-item svg { width: 22px; height: 22px; stroke: currentColor; fill: none; stroke-width: 2; }

  /* ── Layout Principal ── */
  .main-content { margin-left: var(--sidebar-w); padding: 32px 40px; }
  .crm-topbar h1 { font-size: 26px; font-weight: 800; color: var(--text-1); margin-bottom: 24px; }

  /* ── Barra de Filtros Horizontal ── */
  .filter-bar {
    background: #fff;
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 20px 24px;
    margin-bottom: 32px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.04);
  }
  
  /* Ajuste das labels do Streamlit para o design system */
  .stSelectbox label, .stDateInput label {
    font-size: 10px !important;
    font-weight: 700 !important;
    color: var(--text-3) !important;
    text-transform: uppercase !important;
    letter-spacing: .08em !important;
    margin-bottom: 4px !important;
  }

  /* ── Grid e Cards ── */
  .grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 24px; }
  .card {
    background: var(--card-bg); border: 1px solid var(--border); border-radius: 18px;
    padding: 28px; position: relative; overflow: hidden;
    box-shadow: 0 2px 16px rgba(0,0,0,0.04); transition: transform 0.25s;
  }
  .card:hover { transform: translateY(-4px); box-shadow: 0 12px 36px rgba(0,110,255,0.1); }
  .card::before { content: ''; position: absolute; top: 0; left: 0; width: 100%; height: 4px; }
  .card.c-gray::before   { background: var(--text-3); }
  .card.c-purple::before { background: linear-gradient(90deg, var(--purple), #a78bfa); }
  .card.c-green::before  { background: linear-gradient(90deg, var(--green),  #34d399); }
  .card.c-blue::before   { background: linear-gradient(90deg, var(--sky),    #38bdf8); }
  .card.c-orange::before { background: linear-gradient(90deg, var(--orange), #fb923c); }

  .card-icon {
    width: 44px; height: 44px; border-radius: 12px;
    display: flex; align-items: center; justify-content: center; margin-bottom: 18px;
  }
  .card-icon svg { width: 22px; height: 22px; stroke: #fff; fill: none; stroke-width: 2; }
  .bg-gray   { background: var(--text-3); }
  .bg-purple { background: var(--purple); }
  .bg-green  { background: var(--green);  }
  .bg-blue   { background: var(--sky);    }
  .bg-orange { background: var(--orange); }

  .card-label { font-size: 11px; font-weight: 700; color: var(--text-2); text-transform: uppercase; letter-spacing: .1em; margin-bottom: 10px; }
  .card-value { font-size: 32px; font-weight: 800; color: var(--text-1); letter-spacing: -.03em; line-height: 1; margin-bottom: 12px; }
  .card-desc  { font-size: 12px; color: var(--text-3); }
</style>

<!-- Barra Lateral Azul -->
<div class="crm-sidebar">
  <img src="https://logodownload.org/wp-content/uploads/2018/01/farmacias-sao-joao-logo.png" class="logo-img">
  <div class="nav-item"><svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg></div>
  <div class="nav-item active"><svg viewBox="0 0 24 24"><circle cx="9" cy="7" r="4"/><path d="M3 21v-2a4 4 0 0 1 4-4h4a4 4 0 0 1 4 4v2"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/><path d="M21 21v-2a4 4 0 0 0-3-3.87"/></svg></div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# DADOS E DUCKDB
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
# ÁREA PRINCIPAL (Conteúdo)
# ─────────────────────────────────────────────────────────────
st.markdown('<div class="main-content">', unsafe_allow_html=True)

st.markdown('<div class="crm-topbar"><h1>Dashboard CRM</h1></div>', unsafe_allow_html=True)

# Barra de Filtros no Topo
with st.container():
    st.markdown('<div class="filter-bar">', unsafe_allow_html=True)
    
    # Linha 1 dos filtros
    c1, c2, c3, c4, c5 = st.columns([1.2, 1.2, 1, 0.8, 1.5])
    dt_ini = c1.date_input("Data Início", value=date(2025, 1, 1))
    dt_fim = c2.date_input("Data Término", value=date.today())
    canal = c3.selectbox("Canal", ["Total", "Loja", "Digital", "Omnichannel"])
    uf_sel = c4.selectbox("UF", opts("UF"))
    cid_sel = c5.selectbox("Cidade", opts("CIDADE", "UF", uf_sel))
    
    # Linha 2 dos filtros
    c6, c7, c8, c9, c10 = st.columns([1.5, 1, 1, 1, 0.5])
    loj_sel = c6.selectbox("Loja", opts("LOJA", "CIDADE", cid_sel) if cid_sel != "Todas" else opts("LOJA", "UF", uf_sel))
    reg_sel = c7.selectbox("Região", opts("REGIAO"))
    faixa_sel = c8.selectbox("Faixa Etária", ["Todas", "Menor de 24", "Entre 25 e 34", "Entre 35 e 44", "Entre 45 e 54", "Entre 55 e 64", "Mais de 65"])
    sexo_sel = c9.selectbox("Sexo", opts("SEXO"))
    
    if c10.button("Limpar"): st.rerun()
    
    st.markdown('</div>', unsafe_allow_html=True)

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
# EXIBIÇÃO DOS CARDS
# ─────────────────────────────────────────────────────────────
def fmt_n(v): return f"{int(v or 0):,}".replace(",", ".")
def fmt_br(v): return "R$ " + f"{float(v or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

st.markdown(f"""
<div class="grid">
  <div class="card c-gray"><div class="card-icon bg-gray"><svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg></div><div class="card-label">Clientes Totais</div><div class="card-value">{fmt_n(total)}</div><div class="card-desc">Base filtrada.</div></div>
  <div class="card c-purple"><div class="card-icon bg-purple"><svg viewBox="0 0 24 24"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg></div><div class="card-label">Identificados</div><div class="card-value">{fmt_n(ident)}</div><div class="card-desc">Histórico completo.</div></div>
  <div class="card c-green"><div class="card-icon bg-green"><svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg></div><div class="card-label">Clientes Ativos</div><div class="card-value">{fmt_n(ativos)}</div><div class="card-desc">Nos últimos 90 dias.</div></div>
  <div class="card c-blue"><div class="card-icon bg-blue"><svg viewBox="0 0 24 24"><path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="8.5" cy="7" r="4"/><line x1="20" y1="8" x2="20" y2="14"/><line x1="23" y1="11" x2="17" y2="11"/></svg></div><div class="card-label">Novos Clientes</div><div class="card-value">{fmt_n(novos)}</div><div class="card-desc">Período selecionado.</div></div>
  <div class="card c-orange"><div class="card-icon bg-orange"><svg viewBox="0 0 24 24"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg></div><div class="card-label">LTV Médio</div><div class="card-value">{fmt_br(ltv)}</div><div class="card-desc">Receita por cliente.</div></div>
  <div class="card c-purple"><div class="card-icon bg-purple"><svg viewBox="0 0 24 24"><rect x="2" y="5" width="20" height="14" rx="2"/><line x1="2" y1="10" x2="22" y2="10"/></svg></div><div class="card-label">Ticket Médio</div><div class="card-value">{fmt_br(ticket)}</div><div class="card-desc">Por transação.</div></div>
</div>
""", unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)
