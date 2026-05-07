import streamlit as st
import pandas as pd
import glob
import os
from datetime import date, timedelta

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÃO DA PÁGINA
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard CRM — Farmácias São João",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────
# DESIGN SYSTEM (idêntico ao crm.html original)
# ─────────────────────────────────────────────────────────────
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet"/>
<style>
  :root {
    --blue:#006EFF; --bg:#F0F4F8; --card-bg:#FFF; --border:#E5E7EB;
    --text-1:#111827; --text-2:#6B7280; --text-3:#9CA3AF;
    --purple:#7C3AED; --sky:#0EA5E9; --green:#10B981; --orange:#F97316;
  }
  html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; }
  .crm-topbar { margin-bottom: 24px; }
  .crm-topbar h1 { font-size: 24px; font-weight: 800; color: var(--text-1); margin-bottom: 4px; }
  .crm-topbar .sub { font-size: 12px; color: var(--text-3); }
  .grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 24px;
    margin-top: 28px;
  }
  .card {
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 18px;
    padding: 28px;
    position: relative;
    overflow: hidden;
    box-shadow: 0 2px 16px rgba(0,0,0,.04);
    transition: transform .25s, box-shadow .25s;
  }
  .card:hover { transform: translateY(-4px); box-shadow: 0 12px 36px rgba(0,110,255,.10); }
  .card::before { content: ''; position: absolute; top: 0; left: 0; width: 100%; height: 4px; }
  .card.c-gray::before   { background: var(--text-3); }
  .card.c-purple::before { background: linear-gradient(90deg, var(--purple), #a78bfa); }
  .card.c-green::before  { background: linear-gradient(90deg, var(--green),  #34d399); }
  .card.c-blue::before   { background: linear-gradient(90deg, var(--sky),    #38bdf8); }
  .card.c-orange::before { background: linear-gradient(90deg, var(--orange), #fb923c); }
  .card-icon {
    width: 44px; height: 44px; border-radius: 12px;
    display: flex; align-items: center; justify-content: center;
    margin-bottom: 18px;
  }
  .card-icon svg { width: 22px; height: 22px; stroke: #fff; fill: none; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round; }
  .bg-gray   { background: var(--text-3); }
  .bg-purple { background: var(--purple); }
  .bg-green  { background: var(--green);  }
  .bg-blue   { background: var(--sky);    }
  .bg-orange { background: var(--orange); }
  .card-label { font-size: 11px; font-weight: 700; color: var(--text-2); text-transform: uppercase; letter-spacing: .1em; margin-bottom: 10px; }
  .card-value { font-size: 32px; font-weight: 800; color: var(--text-1); letter-spacing: -.03em; line-height: 1; margin-bottom: 12px; }
  .card-desc  { font-size: 12px; color: var(--text-3); line-height: 1.5; }
  @media(max-width:900px) { .grid { grid-template-columns: repeat(2, 1fr); } }
  @media(max-width:600px) { .grid { grid-template-columns: 1fr; } }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# PATH ABSOLUTO — resolve o problema do Streamlit Cloud
# No Cloud, o CWD pode ser diferente de onde está o app.py.
# Usando __file__ garantimos o path correto.
# ─────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# ─────────────────────────────────────────────────────────────
# CARREGAMENTO DE DADOS
# dtype_backend='pyarrow' usa ~260 MB por arquivo (vs 1.1 GB sem ele).
# Para 2 arquivos: ~520 MB total — dentro do limite de 1 GB do Cloud.
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl="1d", show_spinner="Carregando base de clientes…")
def carregar_dados():
    pattern = os.path.join(BASE_DIR, "base_crm_p*.parquet")
    files = sorted(glob.glob(pattern))
    if not files:
        return pd.DataFrame(), []

    dfs = []
    for f in files:
        dfs.append(
            pd.read_parquet(f, engine="pyarrow", dtype_backend="pyarrow")
        )
    df = pd.concat(dfs, ignore_index=True)
    return df, files


df, arquivos_carregados = carregar_dados()

if df.empty:
    st.error("❌ Nenhum arquivo base_crm_p*.parquet encontrado.")
    st.stop()


# ─────────────────────────────────────────────────────────────
# LOOKUP TABLE — apenas colunas categóricas para os filtros
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl="1d")
def build_lookup(_df):
    return (
        _df[["UF", "CIDADE", "LOJA", "REGIAO", "SEXO", "TIPO_PESSOA"]]
        .drop_duplicates()
        .dropna(subset=["UF"])
    )

lkp = build_lookup(df)


def opts(col, filter_col=None, filter_val=None):
    """Lista de opções para selectbox, com cascata."""
    base = lkp
    if filter_col and filter_val and filter_val not in ("Todas", "Todos"):
        base = base[base[filter_col] == filter_val]
    vals = sorted(base[col].dropna().unique().tolist())
    return ["Todas"] + vals


# ─────────────────────────────────────────────────────────────
# BARRA LATERAL — filtros
# ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/4/4b/Logo_Farmacias_Sao_Joao.png",
        width=140,
    )
    st.title("Filtros CRM")

    c1, c2 = st.columns(2)
    dt_inicio = c1.date_input("Data Início", value=date(2025, 1, 1))
    dt_fim    = c2.date_input("Data Término", value=date.today())

    canal = st.selectbox("Canal de Venda", ["Total", "Loja", "Digital", "Omnichannel"])

    st.markdown("---")
    st.subheader("Demográficos")

    uf_sel  = st.selectbox("UF da Loja",     opts("UF"))
    cid_sel = st.selectbox("Cidade da Loja", opts("CIDADE", "UF", uf_sel))
    loj_sel = st.selectbox(
        "Loja",
        opts("LOJA", "CIDADE", cid_sel) if cid_sel != "Todas"
        else opts("LOJA", "UF", uf_sel),
    )
    reg_sel   = st.selectbox("Região", opts("REGIAO"))
    faixa_sel = st.selectbox(
        "Faixa Etária",
        ["Todas", "Menor de 24", "Entre 25 e 34", "Entre 35 e 44",
         "Entre 45 e 54", "Entre 55 e 64", "Mais de 65"],
    )
    sexo_sel = st.selectbox(
        "Sexo",
        ["Todos"] + sorted(lkp["SEXO"].dropna().unique().tolist()),
    )
    tipo_sel = st.selectbox("Tipo de Cliente", opts("TIPO_PESSOA"))


# ─────────────────────────────────────────────────────────────
# FILTRAGEM — máscara booleana (zero cópias do DF completo)
# Com dtype_backend='pyarrow', a comparação com date() funciona
# porque o pyarrow reconhece date32[day] == datetime.date.
# ─────────────────────────────────────────────────────────────
mask = pd.Series(True, index=df.index)

if uf_sel   != "Todas": mask &= df["UF"]         == uf_sel
if cid_sel  != "Todas": mask &= df["CIDADE"]     == cid_sel
if loj_sel  != "Todas": mask &= df["LOJA"]       == loj_sel
if reg_sel  != "Todas": mask &= df["REGIAO"]     == reg_sel
if faixa_sel != "Todas": mask &= df["FAIXA_ETARIA"] == faixa_sel
if sexo_sel  != "Todos": mask &= df["SEXO"]      == sexo_sel
if tipo_sel  != "Todos": mask &= df["TIPO_PESSOA"] == tipo_sel

col_ultima = {
    "Loja": "ULTIMA_COMPRA_LOJA",
    "Digital": "ULTIMA_COMPRA_DIGITAL",
    "Omnichannel": "ULTIMA_COMPRA_OMNI",
}.get(canal, "ULTIMA_COMPRA_GERAL")


# ─────────────────────────────────────────────────────────────
# MÉTRICAS — usando loc[mask, coluna] para não alocar cópia
# ─────────────────────────────────────────────────────────────
qtd_total = int(mask.sum())

if qtd_total > 0:
    limite_ativos = dt_fim - timedelta(days=90)

    pc = df.loc[mask, "PRIMEIRA_COMPRA"]
    uc = df.loc[mask, col_ultima]
    vt = df.loc[mask, "VALOR_TOTAL"]
    tc = df.loc[mask, "TOTAL_COMPRAS"]

    qtd_ident  = int(pc.notna().sum())
    qtd_novos  = int(((pc >= dt_inicio) & (pc <= dt_fim)).sum())
    qtd_ativos = int(((uc >= limite_ativos) & (uc <= dt_fim)).sum())
    ltv_medio    = float(vt.mean())
    total_receita = float(vt.sum())
    total_tickets = float(tc.sum())
    ticket_medio  = total_receita / total_tickets if total_tickets else 0.0
else:
    qtd_ident = qtd_novos = qtd_ativos = 0
    ltv_medio = ticket_medio = 0.0


# ─────────────────────────────────────────────────────────────
# HELPERS DE FORMATAÇÃO
# ─────────────────────────────────────────────────────────────
def fmt_n(v):
    return f"{int(v or 0):,}".replace(",", ".")

def fmt_br(v):
    s = f"{float(v or 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return "R$ " + s


# ─────────────────────────────────────────────────────────────
# LAYOUT PRINCIPAL — design idêntico ao crm.html
# ─────────────────────────────────────────────────────────────
n_files = len(arquivos_carregados)

st.markdown(f"""
<div class="crm-topbar">
  <h1>Dashboard CRM</h1>
  <p class="sub">{n_files} arquivo(s) · {fmt_n(qtd_total)} clientes na seleção</p>
</div>
""", unsafe_allow_html=True)

st.markdown(f"""
<div class="grid">

  <div class="card c-gray">
    <div class="card-icon bg-gray">
      <svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
    </div>
    <div class="card-label">Clientes Totais</div>
    <div class="card-value">{fmt_n(qtd_total)}</div>
    <div class="card-desc">Total de clientes na seleção.</div>
  </div>

  <div class="card c-purple">
    <div class="card-icon bg-purple">
      <svg viewBox="0 0 24 24"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>
    </div>
    <div class="card-label">Clientes Identificados</div>
    <div class="card-value">{fmt_n(qtd_ident)}</div>
    <div class="card-desc">Com primeira compra preenchida.</div>
  </div>

  <div class="card c-green">
    <div class="card-icon bg-green">
      <svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
    </div>
    <div class="card-label">Clientes Ativos</div>
    <div class="card-value">{fmt_n(qtd_ativos)}</div>
    <div class="card-desc">Compras nos últimos 90 dias · canal {canal}.</div>
  </div>

  <div class="card c-blue">
    <div class="card-icon bg-blue">
      <svg viewBox="0 0 24 24"><path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="8.5" cy="7" r="4"/><line x1="20" y1="8" x2="20" y2="14"/><line x1="23" y1="11" x2="17" y2="11"/></svg>
    </div>
    <div class="card-label">Novos Clientes</div>
    <div class="card-value">{fmt_n(qtd_novos)}</div>
    <div class="card-desc">1ª compra no período selecionado.</div>
  </div>

  <div class="card c-orange">
    <div class="card-icon bg-orange">
      <svg viewBox="0 0 24 24"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>
    </div>
    <div class="card-label">LTV Médio</div>
    <div class="card-value">{fmt_br(ltv_medio)}</div>
    <div class="card-desc">Receita média gerada por cliente.</div>
  </div>

  <div class="card c-purple">
    <div class="card-icon bg-purple">
      <svg viewBox="0 0 24 24"><rect x="2" y="5" width="20" height="14" rx="2"/><line x1="2" y1="10" x2="22" y2="10"/></svg>
    </div>
    <div class="card-label">Ticket Médio</div>
    <div class="card-value">{fmt_br(ticket_medio)}</div>
    <div class="card-desc">Valor médio por transação.</div>
  </div>

</div>
""", unsafe_allow_html=True)
