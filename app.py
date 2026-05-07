import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

# Configuração da página
st.set_page_config(page_title="Dashboard CRM - Farmácias São João", layout="wide", initial_sidebar_state="expanded")

# CSS customizado para os cartões bonitos (mantendo o estilo original)
st.markdown("""
<style>
    .metric-card {
        background-color: white;
        border: 1px solid #E5E7EB;
        border-radius: 16px;
        padding: 24px;
        position: relative;
        overflow: hidden;
        box-shadow: 0 2px 10px rgba(0,0,0,.03);
    }
    .metric-card::before {
        content: ''; position: absolute; top: 0; left: 0; width: 100%; height: 4px;
    }
    .c-purple::before { background: linear-gradient(90deg, #7C3AED, #a78bfa); }
    .c-green::before { background: linear-gradient(90deg, #10B981, #34d399); }
    .c-blue::before { background: linear-gradient(90deg, #0EA5E9, #38bdf8); }
    .c-orange::before { background: linear-gradient(90deg, #F97316, #fb923c); }
    .c-gray::before { background: #9CA3AF; }
    
    .lbl { font-size: 12px; font-weight: 700; color: #6B7280; text-transform: uppercase; margin-bottom: 8px; }
    .val { font-size: 32px; font-weight: 800; color: #111827; margin-bottom: 4px; line-height: 1; }
    .desc { font-size: 12px; color: #9CA3AF; }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl="1d")
def carregar_dados():
    try:
        import glob
        files = sorted(glob.glob('base_crm_p*.parquet'))
        if not files:
            # Fallback para o arquivo único antigo se houver
            import os
            if os.path.exists('base_crm.parquet'):
                files = ['base_crm.parquet']
            else:
                st.error("Nenhum arquivo 'base_crm_p*.parquet' encontrado.")
                return pd.DataFrame()
        
        # Carrega todas as partes e junta
        dfs = [pd.read_parquet(f) for f in files]
        df = pd.concat(dfs, ignore_index=True)
        
        # Garantir que colunas de data sejam datetime
        date_cols = ['PRIMEIRA_COMPRA', 'ULTIMA_COMPRA_GERAL', 'ULTIMA_COMPRA_LOJA', 'ULTIMA_COMPRA_DIGITAL', 'ULTIMA_COMPRA_OMNI']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        return df
    except Exception as e:
        st.error(f"Erro ao carregar os dados: {e}")
        return pd.DataFrame()

df_raw = carregar_dados()

if df_raw.empty:
    st.stop()

# --- BARRA LATERAL (FILTROS) ---
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/4/4b/Logo_Farmacias_Sao_Joao.png", width=150)
st.sidebar.title("Filtros CRM")

# Calendário
hoje = datetime.now().date()
inicio_padrao = hoje - timedelta(days=28)

col1, col2 = st.sidebar.columns(2)
data_inicio = col1.date_input("Data Início", value=inicio_padrao)
data_termino = col2.date_input("Data Término", value=hoje)

canal = st.sidebar.selectbox("Canal de Venda", ["Total", "Loja", "Digital", "Omnichannel"])

st.sidebar.markdown("---")
st.sidebar.subheader("Demográficos")

ufs = ["Todas"] + sorted(df_raw['UF'].dropna().unique().tolist())
uf_selecionada = st.sidebar.selectbox("UF da Loja", ufs)

cidades_opcoes = ["Todas"]
if uf_selecionada != "Todas":
    cidades_filtradas = df_raw[df_raw['UF'] == uf_selecionada]['CIDADE'].dropna().unique().tolist()
    cidades_opcoes += sorted(cidades_filtradas)
else:
    cidades_opcoes += sorted(df_raw['CIDADE'].dropna().unique().tolist())
cidade_selecionada = st.sidebar.selectbox("Cidade da Loja", cidades_opcoes)

lojas_opcoes = ["Todas"]
if cidade_selecionada != "Todas":
    lojas_filtradas = df_raw[df_raw['CIDADE'] == cidade_selecionada]['LOJA'].dropna().unique().tolist()
    lojas_opcoes += sorted(lojas_filtradas)
elif uf_selecionada != "Todas":
    lojas_filtradas = df_raw[df_raw['UF'] == uf_selecionada]['LOJA'].dropna().unique().tolist()
    lojas_opcoes += sorted(lojas_filtradas)
loja_selecionada = st.sidebar.selectbox("Loja", lojas_opcoes)

regiao_selecionada = st.sidebar.selectbox("Região", ["Todas"] + sorted(df_raw['REGIAO'].dropna().unique().tolist()))
faixa_etaria = st.sidebar.selectbox("Faixa Etária", ["Todas", "Menor de 24", "Entre 25 e 34", "Entre 35 e 44", "Entre 45 e 54", "Entre 55 e 64", "Mais de 65"])
sexo = st.sidebar.selectbox("Sexo", ["Todos", "Feminino", "Masculino"])
tipo_cliente = st.sidebar.selectbox("Tipo de Cliente", ["Todos"] + sorted(df_raw['TIPO_PESSOA'].dropna().unique().tolist()))

# --- APLICAR FILTROS NO PANDAS ---
df_filtrado = df_raw.copy()

if uf_selecionada != "Todas": df_filtrado = df_filtrado[df_filtrado['UF'] == uf_selecionada]
if cidade_selecionada != "Todas": df_filtrado = df_filtrado[df_filtrado['CIDADE'] == cidade_selecionada]
if loja_selecionada != "Todas": df_filtrado = df_filtrado[df_filtrado['LOJA'] == loja_selecionada]
if regiao_selecionada != "Todas": df_filtrado = df_filtrado[df_filtrado['REGIAO'] == regiao_selecionada]
if faixa_etaria != "Todas": df_filtrado = df_filtrado[df_filtrado['FAIXA_ETARIA'] == faixa_etaria]
if sexo != "Todos": df_filtrado = df_filtrado[df_filtrado['SEXO'] == sexo.upper()] # ou depende da sua base
if tipo_cliente != "Todos": df_filtrado = df_filtrado[df_filtrado['TIPO_PESSOA'] == tipo_cliente]

# Qual data usar para verificar 'Ativos'?
col_ultima = 'ULTIMA_COMPRA_GERAL'
if canal == 'Loja': col_ultima = 'ULTIMA_COMPRA_LOJA'
elif canal == 'Digital': col_ultima = 'ULTIMA_COMPRA_DIGITAL'
elif canal == 'Omnichannel': col_ultima = 'ULTIMA_COMPRA_OMNI'

# Calcular Métricas
qtd_total = len(df_filtrado)
qtd_identificados = df_filtrado['PRIMEIRA_COMPRA'].notna().sum()

# Novos (Primeira compra no período selecionado)
novos_mask = (df_filtrado['PRIMEIRA_COMPRA'] >= data_inicio) & (df_filtrado['PRIMEIRA_COMPRA'] <= data_termino)
qtd_novos = novos_mask.sum()

# Ativos (Última compra no canal nos últimos 90 dias a partir da data de término)
limite_ativos = data_termino - timedelta(days=90)
ativos_mask = (df_filtrado[col_ultima] >= limite_ativos) & (df_filtrado[col_ultima] <= data_termino)
qtd_ativos = ativos_mask.sum()

# Receita e Ticket (Geral da vida do cliente na base filtrada, ou podemos fazer de quem comprou)
ltv_medio = df_filtrado['VALOR_TOTAL'].mean() if qtd_total > 0 else 0
total_receita = df_filtrado['VALOR_TOTAL'].sum()
total_tickets = df_filtrado['TOTAL_COMPRAS'].sum()
ticket_medio = total_receita / total_tickets if total_tickets > 0 else 0

# --- LAYOUT PRINCIPAL ---
st.title("📊 Dashboard CRM - Performance de Clientes")
st.markdown("Dashboard offline otimizado via Parquet. Os filtros na barra lateral são aplicados em memória.")

def fmt_n(v): return f"{int(v):,}".replace(",", ".")
def fmt_b(v): return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

st.markdown("<br>", unsafe_allow_html=True)

# Grid de cartões usando HTML
html_cards = f"""
<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 24px;">
    <div class="metric-card c-gray">
        <div class="lbl">Clientes Totais</div>
        <div class="val">{fmt_n(qtd_total)}</div>
        <div class="desc">Base filtrada</div>
    </div>
    <div class="metric-card c-purple">
        <div class="lbl">Clientes Identificados</div>
        <div class="val">{fmt_n(qtd_identificados)}</div>
        <div class="desc">Primeira compra preenchida</div>
    </div>
    <div class="metric-card c-green">
        <div class="lbl">Clientes Ativos (90 dias)</div>
        <div class="val">{fmt_n(qtd_ativos)}</div>
        <div class="desc">Compraram no canal até 90 dias antes do Término</div>
    </div>
    <div class="metric-card c-blue">
        <div class="lbl">Novos Clientes</div>
        <div class="val">{fmt_n(qtd_novos)}</div>
        <div class="desc">1ª compra entre o Início e o Término</div>
    </div>
    <div class="metric-card c-orange">
        <div class="lbl">LTV Médio</div>
        <div class="val">{fmt_b(ltv_medio)}</div>
        <div class="desc">Receita média por cliente</div>
    </div>
    <div class="metric-card c-purple">
        <div class="lbl">Ticket Médio</div>
        <div class="val">{fmt_b(ticket_medio)}</div>
        <div class="desc">Valor médio por transação</div>
    </div>
</div>
"""
st.markdown(html_cards, unsafe_allow_html=True)
