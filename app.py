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
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────
# DESIGN SYSTEM — idêntico ao crm.html
# ─────────────────────────────────────────────────────────────
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet"/>
<style>
  :root {
    --blue:#006EFF;--bg:#F0F4F8;--card-bg:#FFF;--border:#E5E7EB;
    --text-1:#111827;--text-2:#6B7280;--text-3:#9CA3AF;
    --purple:#7C3AED;--sky:#0EA5E9;--green:#10B981;--orange:#F97316;
  }
  html,body,[class*="css"]{font-family:'Inter',sans-serif!important}
  .crm-topbar{margin-bottom:24px}
  .crm-topbar h1{font-size:24px;font-weight:800;color:var(--text-1);margin-bottom:4px}
  .crm-topbar .sub{font-size:12px;color:var(--text-3)}
  .grid{display:grid;grid-template-columns:repeat(3,1fr);gap:24px;margin-top:28px}
  .card{background:var(--card-bg);border:1px solid var(--border);border-radius:18px;
        padding:28px;position:relative;overflow:hidden;
        box-shadow:0 2px 16px rgba(0,0,0,.04);transition:transform .25s,box-shadow .25s}
  .card:hover{transform:translateY(-4px);box-shadow:0 12px 36px rgba(0,110,255,.10)}
  .card::before{content:'';position:absolute;top:0;left:0;width:100%;height:4px}
  .card.c-gray::before  {background:var(--text-3)}
  .card.c-purple::before{background:linear-gradient(90deg,var(--purple),#a78bfa)}
  .card.c-green::before {background:linear-gradient(90deg,var(--green),#34d399)}
  .card.c-blue::before  {background:linear-gradient(90deg,var(--sky),#38bdf8)}
  .card.c-orange::before{background:linear-gradient(90deg,var(--orange),#fb923c)}
  .card-icon{width:44px;height:44px;border-radius:12px;display:flex;
             align-items:center;justify-content:center;margin-bottom:18px}
  .card-icon svg{width:22px;height:22px;stroke:#fff;fill:none;stroke-width:2;
                 stroke-linecap:round;stroke-linejoin:round}
  .bg-gray  {background:var(--text-3)} .bg-purple{background:var(--purple)}
  .bg-green {background:var(--green)}  .bg-blue  {background:var(--sky)}
  .bg-orange{background:var(--orange)}
  .card-label{font-size:11px;font-weight:700;color:var(--text-2);
              text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px}
  .card-value{font-size:32px;font-weight:800;color:var(--text-1);
              letter-spacing:-.03em;line-height:1;margin-bottom:12px}
  .card-desc{font-size:12px;color:var(--text-3);line-height:1.5}
  @media(max-width:900px){.grid{grid-template-columns:repeat(2,1fr)}}
  @media(max-width:600px){.grid{grid-template-columns:1fr}}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# LOCALIZAÇÃO DOS ARQUIVOS
# ─────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PARQUET_FILES = sorted(glob.glob(os.path.join(BASE_DIR, "base_crm_p*.parquet")))
PARQUET_FILES = [f.replace("\\", "/") for f in PARQUET_FILES]

if not PARQUET_FILES:
    st.error("❌ Nenhum arquivo base_crm_p*.parquet encontrado.")
    st.stop()

# ─────────────────────────────────────────────────────────────
# CONEXÃO DUCKDB
# ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_con(_files):
    return duckdb.connect(database=":memory:")

con = get_con(tuple(PARQUET_FILES))

# ─────────────────────────────────────────────────────────────
# LOOKUP TABLE
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl="1d", show_spinner="Carregando filtros…")
def carregar_lookup(files: tuple):
    sql = """
        SELECT DISTINCT UF, CIDADE, LOJA, REGIAO, SEXO, TIPO_PESSOA
        FROM read_parquet(?)
        WHERE UF IS NOT NULL
    """
    return con.execute(sql, [list(files)]).df()

lkp = carregar_lookup(tuple(PARQUET_FILES))

def opts(col, filter_col=None, filter_val=None):
    base = lkp
    if filter_col and filter_val and filter_val not in ("Todas", "Todos"):
        base = base[base[filter_col] == filter_val]
    vals = sorted(base[col].dropna().unique().tolist())
    return ["Todas"] + vals

# ─────────────────────────────────────────────────────────────
# BARRA LATERAL
# ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/4/4b/Logo_Farmacias_Sao_Joao.png", width=140)
    st.title("Filtros CRM")

    c1, c2 = st.columns(2)
    dt_inicio = c1.date_input("Data Início", value=date(2025, 1, 1))
    dt_fim    = c2.date_input("Data Término", value=date.today())
    canal = st.selectbox("Canal de Venda", ["Total", "Loja", "Digital", "Omnichannel"])

    st.markdown("---")
    st.subheader("Demográficos")
    uf_sel   = st.selectbox("UF da Loja",     opts("UF"))
    cid_sel  = st.selectbox("Cidade da Loja", opts("CIDADE", "UF", uf_sel))
    loj_sel  = st.selectbox("Loja", opts("LOJA", "CIDADE", cid_sel) if cid_sel != "Todas" else opts("LOJA", "UF", uf_sel))
    reg_sel   = st.selectbox("Região", opts("REGIAO"))
    faixa_sel = st.selectbox("Faixa Etária", ["Todas", "Menor de 24", "Entre 25 e 34", "Entre 35 e 44", "Entre 45 e 54", "Entre 55 e 64", "Mais de 65"])
    sexo_sel  = st.selectbox("Sexo", opts("SEXO"))
    tipo_sel  = st.selectbox("Tipo de Cliente", opts("TIPO_PESSOA"))

# ─────────────────────────────────────────────────────────────
# CÁLCULO DE MÉTRICAS
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="Calculando métricas…")
def calcular(files, uf, cid, loj, reg, faixa, sexo, tipo, canal, dt_ini, dt_fim):
    col_ultima = {"Loja": "ULTIMA_COMPRA_LOJA", "Digital": "ULTIMA_COMPRA_DIGITAL", "Omnichannel": "ULTIMA_COMPRA_OMNI"}.get(canal, "ULTIMA_COMPRA_GERAL")
    limite_ativos = dt_fim - timedelta(days=90)

    conds, params = ["1=1"], [list(files)]
    def add(col, val, sentinel):
        if val != sentinel:
            conds.append(f"{col} = ?"); params.append(val)

    add("UF", uf, "Todas")
    add("CIDADE", cid, "Todas")
    add("LOJA", loj, "Todas")
    add("REGIAO", reg, "Todas")
    add("FAIXA_ETARIA", faixa, "Todas")
    add("SEXO", sexo, "Todas")
    add("TIPO_PESSOA", tipo, "Todas")

    where = " AND ".join(conds)
    sql = f"""
    SELECT COUNT(*), COUNT(PRIMEIRA_COMPRA),
           SUM(CASE WHEN PRIMEIRA_COMPRA BETWEEN '{dt_ini}' AND '{dt_fim}' THEN 1 ELSE 0 END),
           SUM(CASE WHEN {col_ultima} BETWEEN '{dt_fim - timedelta(days=90)}' AND '{dt_fim}' THEN 1 ELSE 0 END),
           AVG(VALOR_TOTAL), SUM(VALOR_TOTAL) / NULLIF(SUM(TOTAL_COMPRAS), 0)
    FROM read_parquet(?) WHERE {where}
    """
    return con.execute(sql, params).fetchone()

try:
    total, ident, novos, ativos, ltv, ticket = calcular(tuple(PARQUET_FILES), uf_sel, cid_sel, loj_sel, reg_sel, faixa_sel, sexo_sel, tipo_sel, canal, dt_inicio, dt_fim)
except Exception as e:
    st.error(f"Erro: {e}"); st.stop()

# ─────────────────────────────────────────────────────────────
# LAYOUT PRINCIPAL
# ─────────────────────────────────────────────────────────────
def fmt_n(v): return f"{int(v or 0):,}".replace(",", ".")
def fmt_br(v): return "R$ " + f"{float(v or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

st.markdown(f'<div class="crm-topbar"><h1>Dashboard CRM</h1><p class="sub">{len(PARQUET_FILES)} arquivo(s) · {fmt_n(total)} clientes na seleção</p></div>', unsafe_allow_html=True)
st.markdown(f"""
<div class="grid">
  <div class="card c-gray"><div class="card-icon bg-gray"><svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg></div><div class="card-label">Clientes Totais</div><div class="card-value">{fmt_n(total)}</div><div class="card-desc">Total de clientes na seleção.</div></div>
  <div class="card c-purple"><div class="card-icon bg-purple"><svg viewBox="0 0 24 24"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg></div><div class="card-label">Clientes Identificados</div><div class="card-value">{fmt_n(ident)}</div><div class="card-desc">Com primeira compra preenchida.</div></div>
  <div class="card c-green"><div class="card-icon bg-green"><svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg></div><div class="card-label">Clientes Ativos</div><div class="card-value">{fmt_n(ativos)}</div><div class="card-desc">Compras nos últimos 90 dias · canal {canal}.</div></div>
  <div class="card c-blue"><div class="card-icon bg-blue"><svg viewBox="0 0 24 24"><path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="8.5" cy="7" r="4"/><line x1="20" y1="8" x2="20" y2="14"/><line x1="23" y1="11" x2="17" y2="11"/></svg></div><div class="card-label">Novos Clientes</div><div class="card-value">{fmt_n(novos)}</div><div class="card-desc">1ª compra no período selecionado.</div></div>
  <div class="card c-orange"><div class="card-icon bg-orange"><svg viewBox="0 0 24 24"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg></div><div class="card-label">LTV Médio</div><div class="card-value">{fmt_br(ltv)}</div><div class="card-desc">Receita média gerada por cliente.</div></div>
  <div class="card c-purple"><div class="card-icon bg-purple"><svg viewBox="0 0 24 24"><rect x="2" y="5" width="20" height="14" rx="2"/><line x1="2" y1="10" x2="22" y2="10"/></svg></div><div class="card-label">Ticket Médio</div><div class="card-value">{fmt_br(ticket)}</div><div class="card-desc">Valor médio por transação.</div></div>
</div>
""", unsafe_allow_html=True)
