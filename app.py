import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
import os
import glob

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
        files = sorted(glob.glob('base_crm_p*.parquet'))
        if not files:
            if os.path.exists('base_crm.parquet'):
                files = ['base_crm.parquet']
            else:
                st.error("Arquivos de base (.parquet) não encontrados.")
                return pd.DataFrame()
        
        # OTIMIZAÇÃO CRÍTICA: Usar backend pyarrow para economizar 75% de memória (necessário para Streamlit Cloud 1GB)
        # Isso permite carregar 11M de linhas ocupando apenas ~500MB de RAM.
        dfs = [pd.read_parquet(f, engine='pyarrow', dtype_backend='pyarrow') for f in files]
        df = pd.concat(dfs, ignore_index=True)
        # Garantir que colunas de data sejam datetime (pandas) para comparações corretas
        date_cols = ['PRIMEIRA_COMPRA', 'ULTIMA_COMPRA_GERAL', 'ULTIMA_COMPRA_LOJA', 'ULTIMA_COMPRA_DIGITAL', 'ULTIMA_COMPRA_OMNI']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        return df
    except Exception as e:
        st.error(f"Erro ao carregar os dados: {e}")
        return pd.DataFrame()

df_raw = carregar_dados()

if df_raw.empty:
    st.stop()

# --- PREPARAÇÃO DE FILTROS (OTIMIZADO) ---
@st.cache_data(ttl="1d")
def get_lookup_table(_df):
    # Cria uma tabela minúscula apenas com as combinações únicas de locais
    return _df[['UF', 'CIDADE', 'LOJA', 'REGIAO', 'TIPO_PESSOA']].drop_duplicates().dropna()

df_lookup = get_lookup_table(df_raw)

# --- BARRA LATERAL (FILTROS) ---
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/4/4b/Logo_Farmacias_Sao_Joao.png", width=150)
st.sidebar.title("Filtros CRM")

# Calendário
hoje = date.today()
inicio_padrao = hoje - timedelta(days=28)

col1, col2 = st.sidebar.columns(2)
data_inicio = col1.date_input("Data Início", value=inicio_padrao)
data_termino = col2.date_input("Data Término", value=hoje)

canal = st.sidebar.selectbox("Canal de Venda", ["Total", "Loja", "Digital", "Omnichannel"])

st.sidebar.markdown("---")
st.sidebar.subheader("Demográficos")

# Usamos o df_lookup para preencher os filtros instantaneamente
ufs = ["Todas"] + sorted(df_lookup['UF'].unique().tolist())
uf_selecionada = st.sidebar.selectbox("UF da Loja", ufs)

cidades_opcoes = ["Todas"]
if uf_selecionada != "Todas":
    cidades_filtradas = df_lookup[df_lookup['UF'] == uf_selecionada]['CIDADE'].unique().tolist()
    cidades_opcoes += sorted(cidades_filtradas)
else:
    cidades_opcoes += sorted(df_lookup['CIDADE'].unique().tolist())
cidade_selecionada = st.sidebar.selectbox("Cidade da Loja", cidades_opcoes)

lojas_opcoes = ["Todas"]
if cidade_selecionada != "Todas":
    lojas_filtradas = df_lookup[df_lookup['CIDADE'] == cidade_selecionada]['LOJA'].unique().tolist()
    lojas_opcoes += sorted(lojas_filtradas)
elif uf_selecionada != "Todas":
    lojas_filtradas = df_lookup[df_lookup['UF'] == uf_selecionada]['LOJA'].unique().tolist()
    lojas_opcoes += sorted(lojas_filtradas)
else:
    lojas_opcoes += sorted(df_lookup['LOJA'].unique().tolist())
loja_selecionada = st.sidebar.selectbox("Loja", loja_selecionada if 'loja_selecionada' in locals() else 0, options=lojas_opcoes) if False else st.sidebar.selectbox("Loja", lojas_opcoes)

regiao_selecionada = st.sidebar.selectbox("Região", ["Todas"] + sorted(df_lookup['REGIAO'].unique().tolist()))
faixa_etaria = st.sidebar.selectbox("Faixa Etária", ["Todas", "Menor de 24", "Entre 25 e 34", "Entre 35 e 44", "Entre 45 e 54", "Entre 55 e 64", "Mais de 65"])
sexo = st.sidebar.selectbox("Sexo", ["Todos", "Feminino", "Masculino"])
tipo_cliente = st.sidebar.selectbox("Tipo de Cliente", ["Todos"] + sorted(df_lookup['TIPO_PESSOA'].unique().tolist()))


# --- APLICAR FILTROS NO PANDAS (OTIMIZADO PARA MEMÓRIA) ---
# Em vez de criar cópias do DataFrame, criamos apenas uma máscara booleana.
mask = pd.Series(True, index=df_raw.index)

if uf_selecionada != "Todas": mask &= (df_raw['UF'] == uf_selecionada)
if cidade_selecionada != "Todas": mask &= (df_raw['CIDADE'] == cidade_selecionada)
if loja_selecionada != "Todas": mask &= (df_raw['LOJA'] == loja_selecionada)
if regiao_selecionada != "Todas": mask &= (df_raw['REGIAO'] == regiao_selecionada)
if faixa_etaria != "Todas": mask &= (df_raw['FAIXA_ETARIA'] == faixa_etaria)
if sexo != "Todos": mask &= (df_raw['SEXO'] == sexo.upper())
if tipo_cliente != "Todos": mask &= (df_raw['TIPO_PESSOA'] == tipo_cliente)

# Qual data usar para verificar 'Ativos'?
col_ultima = 'ULTIMA_COMPRA_GERAL'
if canal == 'Loja': col_ultima = 'ULTIMA_COMPRA_LOJA'
elif canal == 'Digital': col_ultima = 'ULTIMA_COMPRA_DIGITAL'
elif canal == 'Omnichannel': col_ultima = 'ULTIMA_COMPRA_OMNI'

# Calcular Métricas usando a máscara (Evita estourar 1GB de RAM)
qtd_total = mask.sum()

# Filtramos apenas as colunas necessárias para os cálculos específicos
if qtd_total > 0:
    qtd_identificados = df_raw.loc[mask, 'PRIMEIRA_COMPRA'].notna().sum()
    
    # Novos (Primeira compra no período selecionado)
    mask_novos = mask & (df_raw['PRIMEIRA_COMPRA'] >= data_inicio) & (df_raw['PRIMEIRA_COMPRA'] <= data_termino)
    qtd_novos = mask_novos.sum()
    
    # Ativos (90 dias no canal)
    limite_ativos = data_termino - timedelta(days=90)
    mask_ativos = mask & (df_raw[col_ultima] >= limite_ativos) & (df_raw[col_ultima] <= data_termino)
    qtd_ativos = mask_ativos.sum()
    
    # Receita e Ticket (usando loc[mask] para economizar memória)
    ltv_medio = df_raw.loc[mask, 'VALOR_TOTAL'].mean()
    total_receita = df_raw.loc[mask, 'VALOR_TOTAL'].sum()
    total_tickets = df_raw.loc[mask, 'TOTAL_COMPRAS'].sum()
    ticket_medio = total_receita / total_tickets if total_tickets > 0 else 0
else:
    qtd_identificados = qtd_novos = qtd_ativos = ltv_medio = ticket_medio = 0

# --- LAYOUT PRINCIPAL ---
st.title("📊 Dashboard CRM - Performance de Clientes")
st.markdown(f"Dashboard carregado com **{qtd_total:,.0f}** clientes. Filtros aplicados em tempo real.".replace(",", "."))

def fmt_n(v): return f"{int(v):,}".replace(",", ".")
def fmt_b(v): return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

st.markdown("<br>", unsafe_allow_html=True)

# Grid de cartões usando HTML
html_cards = f"""
<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 24px;">
    <div class="metric-card c-gray">
        <div class="lbl">Clientes Totais</div>
        <div class="val">{fmt_n(qtd_total)}</div>
        <div class="desc">Base filtrada demograficamente</div>
    </div>
    <div class="metric-card c-purple">
        <div class="lbl">Clientes Identificados</div>
        <div class="val">{fmt_n(qtd_identificados)}</div>
        <div class="desc">Com histórico de compra</div>
    </div>
    <div class="metric-card c-green">
        <div class="lbl">Clientes Ativos (90 dias)</div>
        <div class="val">{fmt_n(qtd_ativos)}</div>
        <div class="desc">Compras no canal {canal}</div>
    </div>
    <div class="metric-card c-blue">
        <div class="lbl">Novos Clientes</div>
        <div class="val">{fmt_n(qtd_novos)}</div>
        <div class="desc">1ª compra no período selecionado</div>
    </div>
    <div class="metric-card c-orange">
        <div class="lbl">LTV Médio</div>
        <div class="val">{fmt_b(ltv_medio)}</div>
        <div class="desc">Receita total média por cliente</div>
    </div>
    <div class="metric-card c-purple">
        <div class="lbl">Ticket Médio</div>
        <div class="val">{fmt_b(ticket_medio)}</div>
        <div class="desc">Gasto médio por transação</div>
    </div>
</div>
"""
st.markdown(html_cards, unsafe_allow_html=True)
