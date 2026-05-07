import streamlit as st
import duckdb
import glob
import os
from datetime import date, timedelta

# ─────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard CRM — Farmácias São João",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────────────────────
# DESIGN SYSTEM COMPLETO
# ─────────────────────────────────────────────────────────────
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet"/>
<style>
  /* ── Variáveis ── */
  :root {
    --blue: #006EFF; --bg: #F0F4F8; --border: #E5E7EB;
    --text-1: #111827; --text-2: #6B7280; --text-3: #9CA3AF;
    --purple: #7C3AED; --sky: #0EA5E9; --green: #10B981; --orange: #F97316;
  }

  /* ── Base ── */
  .stApp { background-color: var(--bg) !important; }
  html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }
  [data-testid="stSidebar"]       { display: none !important; }
  [data-testid="stHeader"]        { background: transparent !important; }
  [data-testid="stDecoration"]    { display: none !important; }

  /* ── Recuo do conteúdo principal para a sidebar azul ── */
  .main .block-container {
    padding-left: 104px !important;
    padding-right: 48px !important;
    padding-top: 36px !important;
    max-width: 100% !important;
  }

  /* ── Sidebar Azul Fixa ── */
  .sidebar-azul {
    position: fixed; top: 0; left: 0;
    width: 80px; height: 100vh;
    background: var(--blue);
    display: flex; flex-direction: column;
    align-items: center; padding: 20px 0;
    z-index: 99999;
    box-shadow: 4px 0 20px rgba(0,110,255,.2);
  }
  .sidebar-azul .logo-box {
    width: 44px; height: 44px; border-radius: 12px;
    background: rgba(255,255,255,.18);
    display: flex; align-items: center; justify-content: center;
    margin-bottom: 36px;
  }
  .sidebar-azul .logo-box span {
    color: #fff; font-weight: 800; font-size: 13px; letter-spacing: -.5px;
  }
  .nav-icon {
    width: 44px; height: 44px; border-radius: 12px;
    display: flex; align-items: center; justify-content: center;
    margin-bottom: 10px; color: rgba(255,255,255,.45); transition: .2s;
  }
  .nav-icon.ativo { background: rgba(255,255,255,.2); color: #fff; }
  .nav-icon svg {
    width: 20px; height: 20px; stroke: currentColor;
    fill: none; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round;
  }

  /* ── Título ── */
  .dash-title {
    font-size: 26px; font-weight: 800;
    color: var(--text-1); margin-bottom: 24px;
  }

  /* ── Formulário de filtros como card branco ── */
  [data-testid="stForm"] {
    background: #fff !important;
    border: 1px solid var(--border) !important;
    border-radius: 16px !important;
    padding: 20px 24px 8px !important;
    box-shadow: 0 2px 12px rgba(0,0,0,.05) !important;
    margin-bottom: 28px !important;
  }

  /* Labels dos widgets dentro do formulário */
  [data-testid="stForm"] label {
    font-size: 10px !important; font-weight: 700 !important;
    color: var(--text-3) !important; text-transform: uppercase !important;
    letter-spacing: .08em !important;
  }

  /* Botão de submit do form */
  [data-testid="stForm"] button[kind="secondaryFormSubmit"],
  [data-testid="stForm"] button[kind="primaryFormSubmit"] {
    background: transparent !important; border: 1px solid var(--border) !important;
    color: var(--text-2) !important; font-size: 13px !important;
    font-weight: 600 !important; border-radius: 8px !important;
    padding: 6px 16px !important; margin-top: 24px !important;
  }

  /* ── Grid de Cards ── */
  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 24px;
    margin-top: 0;
  }
  .kpi-card {
    background: #fff; border-radius: 18px;
    border: 1px solid var(--border);
    padding: 26px 28px;
    position: relative; overflow: hidden;
    box-shadow: 0 2px 16px rgba(0,0,0,.04);
    transition: transform .22s, box-shadow .22s;
  }
  .kpi-card:hover {
    transform: translateY(-4px);
    box-shadow: 0 12px 32px rgba(0,110,255,.1);
  }
  .kpi-card::before {
    content: ''; position: absolute;
    top: 0; left: 0; width: 100%; height: 4px;
  }
  .kpi-card.c-gray::before   { background: var(--text-3); }
  .kpi-card.c-purple::before { background: linear-gradient(90deg, var(--purple), #a78bfa); }
  .kpi-card.c-green::before  { background: linear-gradient(90deg, var(--green),  #34d399); }
  .kpi-card.c-sky::before    { background: linear-gradient(90deg, var(--sky),    #38bdf8); }
  .kpi-card.c-orange::before { background: linear-gradient(90deg, var(--orange), #fb923c); }

  .kpi-icon {
    width: 44px; height: 44px; border-radius: 12px;
    display: flex; align-items: center; justify-content: center;
    margin-bottom: 16px;
  }
  .kpi-icon svg {
    width: 22px; height: 22px; stroke: #fff;
    fill: none; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round;
  }
  .kpi-label {
    font-size: 10px; font-weight: 700; color: var(--text-3);
    text-transform: uppercase; letter-spacing: .1em; margin-bottom: 8px;
  }
  .kpi-value {
    font-size: 30px; font-weight: 800; color: var(--text-1);
    letter-spacing: -.03em; line-height: 1.1; margin-bottom: 10px;
  }
  .kpi-desc { font-size: 12px; color: var(--text-3); line-height: 1.5; }
</style>

<!-- Sidebar Azul (HTML puro, sem imagem externa) -->
<div class="sidebar-azul">
  <div class="logo-box"><span>SJ</span></div>
  <div class="nav-icon">
    <svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
  </div>
  <div class="nav-icon ativo">
    <svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
  </div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# DADOS — DUCKDB (lê parquet em streaming, ~100 MB total)
# ─────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_FILES = sorted(glob.glob(os.path.join(BASE_DIR, "base_crm_p*.parquet")))
PARQUET_FILES = [f.replace("\\", "/") for f in PARQUET_FILES]

if not PARQUET_FILES:
    st.error("❌ Nenhum arquivo base_crm_p*.parquet encontrado.")
    st.stop()

@st.cache_resource
def get_con(_key):
    return duckdb.connect(database=":memory:")

con = get_con(tuple(PARQUET_FILES))

@st.cache_data(ttl="1d", show_spinner="Carregando filtros…")
def lookup(files: tuple):
    return con.execute(
        "SELECT DISTINCT UF, CIDADE, LOJA, REGIAO, SEXO FROM read_parquet(?) WHERE UF IS NOT NULL",
        [list(files)]
    ).df()

lkp = lookup(tuple(PARQUET_FILES))

def opts(col, by=None, val=None):
    base = lkp if (not by or val == "Todas") else lkp[lkp[by] == val]
    return ["Todas"] + sorted(base[col].dropna().unique().tolist())

# ─────────────────────────────────────────────────────────────
# TÍTULO
# ─────────────────────────────────────────────────────────────
st.markdown('<div class="dash-title">Dashboard CRM</div>', unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# FILTROS — usando st.form para ter container estilizável
# ─────────────────────────────────────────────────────────────
with st.form("filtros", clear_on_submit=False):
    r1c1, r1c2, r1c3, r1c4, r1c5 = st.columns([1.1, 1.1, 0.9, 0.7, 1.4])
    dt_ini   = r1c1.date_input("Data Início",  value=date(2025, 1, 1))
    dt_fim   = r1c2.date_input("Data Término", value=date.today())
    canal    = r1c3.selectbox("Canal", ["Total", "Loja", "Digital", "Omnichannel"])
    uf_sel   = r1c4.selectbox("UF", opts("UF"))
    cid_sel  = r1c5.selectbox("Cidade", opts("CIDADE", "UF", uf_sel))

    r2c1, r2c2, r2c3, r2c4, r2c5 = st.columns([1.4, 0.9, 1.2, 0.9, 0.5])
    loj_sel   = r2c1.selectbox("Loja", opts("LOJA", "CIDADE", cid_sel) if cid_sel != "Todas" else opts("LOJA", "UF", uf_sel))
    reg_sel   = r2c2.selectbox("Região", opts("REGIAO"))
    faixa_sel = r2c3.selectbox("Faixa Etária", ["Todas","Menor de 24","Entre 25 e 34","Entre 35 e 44","Entre 45 e 54","Entre 55 e 64","Mais de 65"])
    sexo_sel  = r2c4.selectbox("Sexo", opts("SEXO"))
    r2c5.form_submit_button("Limpar")

# ─────────────────────────────────────────────────────────────
# MÉTRICAS — SQL via DuckDB (streaming, sem pandas na memória)
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="Calculando…")
def calcular(files, uf, cid, loj, reg, faixa, sexo, canal, dt_i, dt_f):
    col_u = {"Loja": "ULTIMA_COMPRA_LOJA", "Digital": "ULTIMA_COMPRA_DIGITAL",
             "Omnichannel": "ULTIMA_COMPRA_OMNI"}.get(canal, "ULTIMA_COMPRA_GERAL")
    conds, params = ["1=1"], [list(files)]
    def add(col, v):
        if v != "Todas": conds.append(f"{col} = ?"); params.append(v)
    add("UF", uf); add("CIDADE", cid); add("LOJA", loj)
    add("REGIAO", reg); add("FAIXA_ETARIA", faixa); add("SEXO", sexo)
    lim = dt_f - timedelta(days=90)
    sql = f"""
    SELECT COUNT(*), COUNT(PRIMEIRA_COMPRA),
           SUM(CASE WHEN PRIMEIRA_COMPRA BETWEEN '{dt_i}' AND '{dt_f}' THEN 1 ELSE 0 END),
           SUM(CASE WHEN {col_u} BETWEEN '{lim}' AND '{dt_f}' THEN 1 ELSE 0 END),
           AVG(VALOR_TOTAL),
           SUM(VALOR_TOTAL) / NULLIF(SUM(TOTAL_COMPRAS), 0)
    FROM read_parquet(?) WHERE {" AND ".join(conds)}
    """
    return con.execute(sql, params).fetchone()

total, ident, novos, ativos, ltv, ticket = calcular(
    tuple(PARQUET_FILES),
    uf_sel, cid_sel, loj_sel, reg_sel, faixa_sel, sexo_sel,
    canal, dt_ini, dt_fim,
)

# ─────────────────────────────────────────────────────────────
# FORMATAÇÃO
# ─────────────────────────────────────────────────────────────
def N(v):  return f"{int(v or 0):,}".replace(",", ".")
def R(v):  return "R$ " + f"{float(v or 0):,.2f}".replace(",","X").replace(".",",").replace("X",".")

# ─────────────────────────────────────────────────────────────
# CARDS DE KPI
# ─────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="kpi-grid">

  <div class="kpi-card c-gray">
    <div class="kpi-icon" style="background:#9CA3AF">
      <svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
    </div>
    <div class="kpi-label">Clientes Totais</div>
    <div class="kpi-value">{N(total)}</div>
    <div class="kpi-desc">Total de clientes na seleção.</div>
  </div>

  <div class="kpi-card c-purple">
    <div class="kpi-icon" style="background:#7C3AED">
      <svg viewBox="0 0 24 24"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>
    </div>
    <div class="kpi-label">Clientes Identificados</div>
    <div class="kpi-value">{N(ident)}</div>
    <div class="kpi-desc">Com primeira compra preenchida.</div>
  </div>

  <div class="kpi-card c-green">
    <div class="kpi-icon" style="background:#10B981">
      <svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
    </div>
    <div class="kpi-label">Clientes Ativos</div>
    <div class="kpi-value">{N(ativos)}</div>
    <div class="kpi-desc">Compras nos últimos 90 dias · canal {canal}.</div>
  </div>

  <div class="kpi-card c-sky">
    <div class="kpi-icon" style="background:#0EA5E9">
      <svg viewBox="0 0 24 24"><path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="8.5" cy="7" r="4"/><line x1="20" y1="8" x2="20" y2="14"/><line x1="23" y1="11" x2="17" y2="11"/></svg>
    </div>
    <div class="kpi-label">Novos Clientes</div>
    <div class="kpi-value">{N(novos)}</div>
    <div class="kpi-desc">1ª compra no período selecionado.</div>
  </div>

  <div class="kpi-card c-orange">
    <div class="kpi-icon" style="background:#F97316">
      <svg viewBox="0 0 24 24"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>
    </div>
    <div class="kpi-label">LTV Médio</div>
    <div class="kpi-value">{R(ltv)}</div>
    <div class="kpi-desc">Receita média gerada por cliente.</div>
  </div>

  <div class="kpi-card c-purple">
    <div class="kpi-icon" style="background:#7C3AED">
      <svg viewBox="0 0 24 24"><rect x="2" y="5" width="20" height="14" rx="2"/><line x1="2" y1="10" x2="22" y2="10"/></svg>
    </div>
    <div class="kpi-label">Ticket Médio</div>
    <div class="kpi-value">{R(ticket)}</div>
    <div class="kpi-desc">Valor médio por transação.</div>
  </div>

</div>
""", unsafe_allow_html=True)
