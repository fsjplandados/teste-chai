import streamlit as st
import pandas as pd
import duckdb
from datetime import date, timedelta

# ─────────────────────────────────────────────────────────────
# CONFIGURAÇÃO E DESIGN SYSTEM SIMPLIFICADO
# ─────────────────────────────────────────────────────────────
st.set_page_config(page_title="Dashboard CRM — Alinhamento", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;700;800&display=swap" rel="stylesheet"/>
<style>
    :root { --blue: #006EFF; --bg: #F0F4F8; --border: #E5E7EB; --text: #111827; }
    .stApp { background-color: var(--bg) !important; font-family: 'Inter', sans-serif !important; }
    [data-testid="stSidebar"], [data-testid="stHeader"] { display: none !important; }
    .main .block-container { padding: 40px 80px !important; }
    
    .kpi-card { background: #fff; border: 1px solid var(--border); border-radius: 16px; padding: 32px; box-shadow: 0 4px 12px rgba(0,0,0,0.03); margin-bottom: 24px; }
    .kpi-label { font-size: 12px; font-weight: 700; color: #6B7280; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 8px; }
    .kpi-value { font-size: 42px; font-weight: 800; color: var(--text); }
    .kpi-sub { font-size: 13px; color: #9CA3AF; margin-top: 8px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# LOGICA DE DADOS (FOCO TOTAL EM CLIENTES)
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def get_clientes_data(d_f, uf, cid, reg, sx, lj):
    con = duckdb.connect()
    # Usando a base original para alinhamento direto
    source = "read_parquet('base_crm_p*.parquet')"
    
    # Filtros de Dimensão
    where_cl = []
    if uf != "Todas": where_cl.append(f"UF = '{uf}'")
    if cid != "Todas": where_cl.append(f"CIDADE = '{cid}'")
    if reg != "Todas": where_cl.append(f"REGIAO = '{reg}'")
    if sx != "Todas": where_cl.append(f"SEXO = '{sx}'")
    if lj != "Todas": where_cl.append(f"LOJA = '{lj}'")
    
    where_str = " AND ".join(where_cl) if where_cl else "1=1"
    
    # 1. Clientes Totais (Sem filtro de data, apenas dimensões)
    totais = con.execute(f"SELECT COUNT(*) FROM {source} WHERE {where_str}").fetchone()[0]
    
    # 2. Clientes Ativos 90d (Em relação à data selecionada)
    d_90 = d_f - timedelta(days=90)
    ativos = con.execute(f"""
        SELECT COUNT(*) FROM {source} 
        WHERE {where_str} 
        AND ULTIMA_COMPRA_GERAL >= '{d_90}' 
        AND ULTIMA_COMPRA_GERAL <= '{d_f}'
    """).fetchone()[0]
    
    return totais, ativos

# ─────────────────────────────────────────────────────────────
# INTERFACE DE ALINHAMENTO
# ─────────────────────────────────────────────────────────────
st.title("🎯 Alinhamento de KPIs — Snowflake")
st.write("Foco: Validação de Clientes Totais e Ativos (Janela 90 dias)")

with st.form("filtros_alinhamento"):
    c1, c2, c3 = st.columns([1, 1, 1])
    data_referencia = c1.date_input("Data de Referência (Fim)", value=date(2026, 1, 31))
    uf_sel = c2.selectbox("UF", ["Todas", "RS", "SC", "PR"])
    
    con_tmp = duckdb.connect()
    cidades = ["Todas"] + sorted(con_tmp.execute(f"SELECT DISTINCT CIDADE FROM read_parquet('base_crm_p*.parquet') WHERE CIDADE IS NOT NULL {'AND UF = '+chr(39)+uf_sel+chr(39) if uf_sel != 'Todas' else ''}").df()['CIDADE'].tolist())
    cid_sel = c3.selectbox("Cidade", cidades)
    
    c4, c5, c6 = st.columns(3)
    reg_sel = c4.selectbox("Região", ["Todas", "Serra", "Litoral", "Metropolitana", "Interior"])
    sexo_sel = c5.selectbox("Sexo", ["Todas", "M", "F"])
    loja_sel = c6.selectbox("Loja", ["Todas", "Loja 01", "Loja 02"])
    
    btn_atu = st.form_submit_button("Atualizar e Validar", type="primary")

try:
    totais, ativos = get_clientes_data(data_referencia, uf_sel, cid_sel, reg_sel, sexo_sel, loja_sel)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">Clientes Totais</div>
                <div class="kpi-value">{totais:,}</div>
                <div class="kpi-sub">SELECT COUNT(*) FROM DATAMART_CLIENTES</div>
            </div>
        """, unsafe_allow_html=True)
        
    with col2:
        st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">Clientes Ativos (90 dias)</div>
                <div class="kpi-value">{ativos:,}</div>
                <div class="kpi-sub">Última compra entre {(data_referencia - timedelta(days=90)).strftime('%d/%m/%Y')} e {data_referencia.strftime('%d/%m/%Y')}</div>
            </div>
        """, unsafe_allow_html=True)

    st.success("💡 Dica: Para bater com o Snowflake, utilize a mesma 'Data de Referência' no SQL de lá.")

except Exception as e:
    st.error(f"Erro ao carregar dados: {e}")
