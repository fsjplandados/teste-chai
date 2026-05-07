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

st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet"/>
<style>
    :root {
        --blue: #006EFF; --bg: #F0F4F8; --card-bg: #FFF; --border: #E5E7EB;
        --text-1: #111827; --text-2: #6B7280; --text-3: #9CA3AF;
        --purple: #7C3AED; --sky: #0EA5E9; --green: #10B981; --red: #EF4444; --orange: #F97316;
    }

    /* Base */
    .stApp { background-color: var(--bg) !important; font-family: 'Inter', sans-serif !important; }
    [data-testid="stSidebar"], [data-testid="stHeader"], [data-testid="stDecoration"] { display: none !important; }
    
    .main .block-container {
        padding: 24px 48px 24px 110px !important;
        max-width: 100% !important;
    }

    /* Sidebar Azul */
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
        margin-bottom: 30px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); padding: 6px;
    }
    .logo-circle img { width: 100%; height: auto; object-fit: contain; }
    
    .nav-item {
        width: 48px; height: 48px; border-radius: 12px;
        display: flex; align-items: center; justify-content: center;
        color: rgba(255,255,255,0.4); margin-bottom: 12px; transition: 0.2s;
    }
    .nav-item.active { background: rgba(255,255,255,0.2); color: #fff; }
    .nav-item svg { width: 22px; height: 22px; stroke: currentColor; fill: none; stroke-width: 2; }

    /* Barra de Filtros Inteligente */
    div[data-testid="stForm"] {
        background: #fff !important; border: 1px solid var(--border) !important;
        border-radius: 16px !important; padding: 24px 32px !important;
        margin-bottom: 32px !important; box-shadow: 0 4px 20px rgba(0,0,0,0.03) !important;
    }
    .stSelectbox label, .stDateInput label {
        font-size: 11px !important; font-weight: 700 !important;
        color: var(--text-2) !important; text-transform: uppercase !important;
        letter-spacing: .06em !important; margin-bottom: 8px !important;
    }
    .filter-section-title {
        font-size: 13px; font-weight: 700; color: var(--text-1);
        margin-bottom: 12px; border-bottom: 1px solid var(--border); padding-bottom: 8px;
    }

    /* Cards e Deltas */
    .kpi-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 24px; }
    .kpi-card {
        background: #fff; border: 1px solid var(--border); border-radius: 18px;
        padding: 24px 28px; position: relative; overflow: hidden;
        box-shadow: 0 2px 16px rgba(0,0,0,0.04);
    }
    .kpi-card::before { content: ''; position: absolute; top: 0; left: 0; width: 100%; height: 4px; }
    .kpi-icon {
        width: 40px; height: 40px; border-radius: 10px;
        display: flex; align-items: center; justify-content: center; margin-bottom: 16px;
    }
    .kpi-icon svg { width: 20px; height: 20px; stroke: #fff; fill: none; stroke-width: 2; }
    .kpi-label { font-size: 10px; font-weight: 700; color: var(--text-3); text-transform: uppercase; letter-spacing: .1em; }
    .kpi-value-container { display: flex; align-items: baseline; gap: 12px; margin: 6px 0; }
    .kpi-value { font-size: 28px; font-weight: 800; color: var(--text-1); letter-spacing: -0.5px; }
    
    .delta {
        font-size: 11px; font-weight: 700; padding: 2px 8px; border-radius: 6px;
        display: flex; align-items: center; gap: 4px;
    }
    .delta.up { background: rgba(16, 185, 129, 0.1); color: var(--green); }
    .delta.down { background: rgba(239, 68, 68, 0.1); color: var(--red); }

    /* Botão */
    div[data-testid="stForm"] button {
        background-color: var(--blue) !important; color: white !important;
        border-radius: 10px !important; padding: 12px 24px !important;
        font-weight: 700 !important; width: 100% !important; margin-top: 10px !important;
        box-shadow: 0 4px 12px rgba(0,110,255,0.2) !important;
    }
</style>

<div class="crm-sidebar">
    <div class="logo-circle"><img src="https://upload.wikimedia.org/wikipedia/commons/4/4b/Logo_Farmacias_Sao_Joao.png"></div>
    <div class="nav-item active"><svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg></div>
    <div class="nav-item"><svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg></div>
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
    return con.execute("SELECT DISTINCT UF, CIDADE, SEXO, TIPO_PESSOA FROM read_parquet(?) WHERE UF IS NOT NULL", [list(files)]).df()

lkp = carregar_lookup(tuple(PARQUET_FILES))

def opts(col, by=None, val=None):
    base = lkp if (not by or val == "Todas") else lkp[lkp[by] == val]
    return ["Todas"] + sorted(base[col].dropna().unique().tolist())

# ─────────────────────────────────────────────────────────────
# INTERFACE - COMPARATIVO INTELIGENTE
# ─────────────────────────────────────────────────────────────
st.markdown('<div style="font-size:24px; font-weight:800; color:#111827; margin-bottom:20px;">Dashboard CRM</div>', unsafe_allow_html=True)

with st.form("filtros_form"):
    st.markdown('<div class="filter-section-title">📅 Período e Comparação</div>', unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1.5, 1.5, 1])
    
    hoje = date.today()
    # 1. Seleção Principal (Calendário Interativo)
    periodo_range = c1.date_input("Período Principal", value=(hoje - timedelta(days=28), hoje))
    
    # Extração de datas e lógica de sugestão
    if isinstance(periodo_range, (list, tuple)) and len(periodo_range) == 2:
        dt_ini_p, dt_fim_p = periodo_range
        is_intervalo = dt_ini_p != dt_fim_p
    else:
        dt_ini_p = dt_fim_p = periodo_range[0] if isinstance(periodo_range, (list, tuple)) else periodo_range
        is_intervalo = False

    # 2. Comparativo Inteligente Automático
    if not is_intervalo:
        opcoes_comp = ["Ontem", "7 dias atrás", "Mês anterior", "Ano anterior", "Sem comparação"]
    else:
        opcoes_comp = ["Período anterior", "Mês anterior", "Ano anterior", "Sem comparação"]
    
    comparar_com = c2.selectbox("Comparativo Sugerido", opcoes_comp)
    
    # Filtros de Segmentação
    st.markdown('<div style="margin-top:20px;" class="filter-section-title">🔍 Segmentação</div>', unsafe_allow_html=True)
    f1, f2, f3, f4, f5, f6 = st.columns([1, 0.8, 1.2, 0.8, 0.8, 0.5])
    canal  = f1.selectbox("Canal", ["Total", "Loja", "Digital", "Omnichannel"])
    uf_sel = f2.selectbox("UF", opts("UF"))
    cid_sel = f3.selectbox("Cidade", opts("CIDADE", "UF", uf_sel))
    sexo_sel = f4.selectbox("Sexo", opts("SEXO"))
    tipo_sel = f5.selectbox("Tipo Cliente", opts("TIPO_PESSOA"))
    btn = f6.form_submit_button("Aplicar")

# Lógica de cálculo das datas de comparação baseada na sugestão inteligente
dias_p = (dt_fim_p - dt_ini_p).days + 1
dt_ini_c = dt_fim_c = None

if comparar_com == "Ontem":
    dt_ini_c = dt_fim_c = hoje - timedelta(days=1)
elif comparar_com == "7 dias atrás":
    dt_ini_c = dt_fim_c = dt_ini_p - timedelta(days=7)
elif comparar_com == "Período anterior":
    dt_ini_c, dt_fim_c = dt_ini_p - timedelta(days=dias_p), dt_ini_p - timedelta(days=1)
elif comparar_com == "Mês anterior":
    dt_ini_c, dt_fim_c = dt_ini_p - relativedelta(months=1), dt_fim_p - relativedelta(months=1)
elif comparar_com == "Ano anterior":
    dt_ini_c, dt_fim_c = dt_ini_p - relativedelta(years=1), dt_fim_p - relativedelta(years=1)

# ─────────────────────────────────────────────────────────────
# CÁLCULOS
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def calcular_metrics(files, uf, cid, sexo, tipo, canal, d_i, d_f):
    if not d_i or not d_f: return [0,0,0,0,0,0]
    col_u = {"Loja": "ULTIMA_COMPRA_LOJA", "Digital": "ULTIMA_COMPRA_DIGITAL", "Omnichannel": "ULTIMA_COMPRA_OMNI"}.get(canal, "ULTIMA_COMPRA_GERAL")
    conds, params = [f"{col_u} BETWEEN ? AND ?"], [list(files), d_i.strftime("%Y-%m-%d"), d_f.strftime("%Y-%m-%d")]
    def add(col, v):
        if v != "Todas": conds.append(f"{col} = ?"); params.append(v)
    add("UF", uf); add("CIDADE", cid); add("SEXO", sexo); add("TIPO_PESSOA", tipo)
    
    lim_ativos = d_f - timedelta(days=90)
    sql = f"""
    SELECT COUNT(*), COUNT(PRIMEIRA_COMPRA),
           SUM(CASE WHEN PRIMEIRA_COMPRA BETWEEN '{d_i}' AND '{d_f}' THEN 1 ELSE 0 END),
           SUM(CASE WHEN {col_u} BETWEEN '{lim_ativos}' AND '{d_f}' THEN 1 ELSE 0 END),
           AVG(VALOR_TOTAL), SUM(VALOR_TOTAL) / NULLIF(SUM(TOTAL_COMPRAS), 0)
    FROM read_parquet(?) WHERE {" AND ".join(conds)}
    """
    return con.execute(sql, params).fetchone()

m_at = calcular_metrics(tuple(PARQUET_FILES), uf_sel, cid_sel, sexo_sel, tipo_sel, canal, dt_ini_p, dt_fim_p)
m_ant = calcular_metrics(tuple(PARQUET_FILES), uf_sel, cid_sel, sexo_sel, tipo_sel, canal, dt_ini_c, dt_fim_c)

# ─────────────────────────────────────────────────────────────
# UI - EXIBIÇÃO
# ─────────────────────────────────────────────────────────────
def card(label, val, prev_val, is_currency=False, color_class="c-gray", icon_svg=""):
    diff = ((val / prev_val) - 1) * 100 if prev_val and prev_val > 0 else 0
    delta_class = "up" if diff >= 0 else "down"
    delta_icon = "▲" if diff >= 0 else "▼"
    
    fmt_v = (f"R$ {val:,.2f}" if is_currency else f"{int(val):,}")
    fmt_v = fmt_v.replace(",", "X").replace(".", ",").replace("X", ".")
    
    delta_html = f'<div class="delta {delta_class}">{delta_icon} {abs(diff):.1f}%</div>' if dt_ini_c else ""
    desc_txt = f"vs. {comparar_com.lower()}" if dt_ini_c else ""
    
    st.markdown(f"""
    <div class="kpi-card {color_class}">
        <div class="kpi-icon" style="background:var(--{color_class.split('-')[1]})">{icon_svg}</div>
        <div class="kpi-label">{label}</div>
        <div class="kpi-value-container">
            <div class="kpi-value">{fmt_v}</div>
            {delta_html}
        </div>
        <div class="kpi-desc" style="font-size:10px; color:var(--text-3); font-weight:500;">{desc_txt}</div>
    </div>
    """, unsafe_allow_html=True)

# Ícones
i_user = '<svg viewBox="0 0 24 24"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>'
i_ident = '<svg viewBox="0 0 24 24"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>'
i_pulse = '<svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>'
i_plus = '<svg viewBox="0 0 24 24"><path d="M16 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="8.5" cy="7" r="4"/><line x1="20" y1="8" x2="20" y2="14"/><line x1="23" y1="11" x2="17" y2="11"/></svg>'
i_money = '<svg viewBox="0 0 24 24"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>'
i_card = '<svg viewBox="0 0 24 24"><rect x="2" y="5" width="20" height="14" rx="2"/><line x1="2" y1="10" x2="22" y2="10"/></svg>'

c1, c2, c3 = st.columns(3)
with c1: card("Clientes Totais", m_at[0], m_ant[0], False, "c-gray", i_user)
with c2: card("Identificados", m_at[1], m_ant[1], False, "c-purple", i_ident)
with c3: card("Clientes Ativos", m_at[3], m_ant[3], False, "c-green", i_pulse)
st.write("")
c4, c5, c6 = st.columns(3)
with c4: card("Novos Clientes", m_at[2], m_ant[2], False, "c-blue", i_plus)
with c5: card("LTV Médio", m_at[4], m_ant[4], True, "c-orange", i_money)
with c6: card("Ticket Médio", m_at[5], m_ant[5], True, "c-purple", i_card)
