import pyodbc
import json
import os
from datetime import datetime

DSN = 'SNOWFLAKE_FSJ'
OUT = 'dashboard'

# Mapeamento estático para garantir que a cidade seja a da LOJA de preferência, e não do cadastro do cliente.
FILTROS_MAP = {
    'uf': 'L.UF_CIDADE',
    'cidade': 'L.NOME_CIDADE',
    'loja': 'L.LOJA_NOME',
    'regiao': 'L.REGIAO_NOME', # Usando a regiao da loja
    'sexo': 'C.SEXO',
    'faixa_etaria': 'C.IDADE', # Será tratado no CASE
    'canal': 'C.CANAL_PREFERENCIAL', # Coluna genérica que pode existir, faremos fallback
    'tipo_cliente': 'C.TIPO_PESSOA'
}

def run():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Conectando ao Snowflake...")
    try:
        conn = pyodbc.connect(f"DSN={DSN}")
        cur = conn.cursor()
        print("Conectado com sucesso!\n")
    except Exception as e:
        print(f"Erro ao conectar: {e}")
        return

    # Descobrir quais colunas realmente existem no Datamart para os filtros diretos do cliente
    cur.execute("""
        SELECT COLUMN_NAME FROM FSJ_PRD.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='GOLD' AND TABLE_NAME='DATAMART_CLIENTES'
    """)
    colunas_dm = [r[0].upper() for r in cur.fetchall()]
    
    # Tratamento customizado para IDADE -> FAIXA ETARIA
    if 'IDADE' in colunas_dm:
        FILTROS_MAP['faixa_etaria'] = """
            CASE 
                WHEN C.IDADE < 24 THEN 'Menor de 24'
                WHEN C.IDADE BETWEEN 25 AND 34 THEN 'Entre 25 e 34'
                WHEN C.IDADE BETWEEN 35 AND 44 THEN 'Entre 35 e 44'
                WHEN C.IDADE BETWEEN 45 AND 54 THEN 'Entre 45 e 54'
                WHEN C.IDADE BETWEEN 55 AND 64 THEN 'Entre 55 e 64'
                WHEN C.IDADE >= 65 THEN 'Mais de 65'
                ELSE 'Não Informado'
            END
        """
    elif 'FAIXA_ETARIA' in colunas_dm:
        FILTROS_MAP['faixa_etaria'] = 'C.FAIXA_ETARIA'
    else:
        del FILTROS_MAP['faixa_etaria']
        
    if 'TIPO_PESSOA' not in colunas_dm:
        del FILTROS_MAP['tipo_cliente']

    if 'SEXO' not in colunas_dm and 'GENERO' not in colunas_dm:
        del FILTROS_MAP['sexo']
    elif 'SEXO' not in colunas_dm and 'GENERO' in colunas_dm:
        FILTROS_MAP['sexo'] = 'C.GENERO'
        
    # Verificar Canal (usualmente não está direto no Datamart, mas vamos tentar)
    canal_cols = [c for c in colunas_dm if 'CANAL' in c]
    if canal_cols:
        FILTROS_MAP['canal'] = f"C.{canal_cols[0]}"
    else:
        del FILTROS_MAP['canal']

    print(f"Filtros configurados: {list(FILTROS_MAP.keys())}")

    # Base Query e Metricas
    # A junção com VW_LOJAS resolve o problema de usar a UF e CIDADE onde o cliente compra.
    from_clause = """
        FSJ_PRD.GOLD.DATAMART_CLIENTES C
        LEFT JOIN FSJ_PRD.GOLD.VW_LOJAS L ON C.LOJA_ID_PREFERENCIA = L.LOJA_ID
    """
    
    metricas_base = """
        COUNT(*) AS total,
        COUNT(CASE WHEN C.PRIMEIRA_COMPRA IS NOT NULL THEN 1 END) AS identificados,
        COUNT(CASE WHEN C.ULTIMA_COMPRA >= DATEADD(DAY,-90,CURRENT_DATE()) THEN 1 END) AS ativos,
        COUNT(CASE WHEN C.PRIMEIRA_COMPRA >= DATEADD(DAY,-30,CURRENT_DATE()) THEN 1 END) AS novos,
        ROUND(AVG(CASE WHEN C.VALOR_TOTAL > 0 THEN C.VALOR_TOTAL END),2) AS ltv,
        ROUND(CASE WHEN SUM(CASE WHEN C.TOTAL_COMPRAS>0 THEN C.TOTAL_COMPRAS END)>0
              THEN SUM(CASE WHEN C.TOTAL_COMPRAS>0 THEN C.VALOR_TOTAL END)/SUM(CASE WHEN C.TOTAL_COMPRAS>0 THEN C.TOTAL_COMPRAS END)
              ELSE 0 END,2) AS ticket
    """

    print("\n=== Extraindo mapeamento UF -> Cidade das LOJAS ===")
    uf_cidade = {}
    if 'uf' in FILTROS_MAP and 'cidade' in FILTROS_MAP:
        cur.execute(f"SELECT DISTINCT L.UF_CIDADE, L.NOME_CIDADE FROM {from_clause} WHERE L.UF_CIDADE IS NOT NULL AND L.NOME_CIDADE IS NOT NULL")
        for r in cur.fetchall():
            u, c = str(r[0]).strip(), str(r[1]).strip()
            if u not in uf_cidade: uf_cidade[u] = []
            uf_cidade[u].append(c)
        for k in uf_cidade: uf_cidade[k].sort()

    print("\n=== Pre-agregando metricas ===")
    cur.execute(f"SELECT {metricas_base} FROM {from_clause}")
    r = cur.fetchone()
    totais = {'total':float(r[0] or 0),'identificados':float(r[1] or 0),'ativos':float(r[2] or 0),
              'novos':float(r[3] or 0),'ltv':float(r[4] or 0),'ticket':float(r[5] or 0)}

    dados_filtro = {}
    opcoes_filtro = {}
    
    for nome, col_sql in FILTROS_MAP.items():
        print(f"  Processando {nome}...")
        try:
            cur.execute(f"SELECT {col_sql} AS dim, {metricas_base} FROM {from_clause} GROUP BY 1 ORDER BY 1 LIMIT 500")
            rows = []
            opts = []
            for row in cur.fetchall():
                dim_val = str(row[0]).strip() if row[0] else 'Não Informado'
                if dim_val not in opts and dim_val != 'Não Informado' and dim_val != 'None': 
                    opts.append(dim_val)
                rows.append({
                    'dim': dim_val, 'total': float(row[1] or 0),
                    'identificados': float(row[2] or 0), 'ativos': float(row[3] or 0),
                    'novos': float(row[4] or 0), 'ltv': float(row[5] or 0), 'ticket': float(row[6] or 0)
                })
            dados_filtro[nome] = rows
            opts.sort()
            opcoes_filtro[nome] = opts
        except Exception as e:
            print(f"  -> ERRO ao agregar {nome}: {e}")

    conn.close()

    js_content = f"""
// Arquivo gerado automaticamente pelo Python ETL
const LAST_UPDATE = "{datetime.now().strftime('%d/%m/%Y %H:%M')}";
const TOTAIS = {json.dumps(totais)};
const UF_CIDADE = {json.dumps(uf_cidade)};
const OPCOES = {json.dumps(opcoes_filtro)};
const DADOS = {json.dumps(dados_filtro)};
"""
    os.makedirs(OUT, exist_ok=True)
    with open(f'{OUT}/data.js', 'w', encoding='utf-8') as f:
        f.write(js_content)
        
    gerar_html(list(FILTROS_MAP.keys()))
    print(f"\nDashboard gerado: {OUT}/crm.html e {OUT}/data.js")

def gerar_html(filtros_keys):
    lbls = {'canal':'Canal', 'uf':'UF', 'cidade':'Cidade', 'loja':'Loja', 
            'tipo_cliente':'Tipo Cliente', 'sexo':'Sexo', 'faixa_etaria':'Faixa Etária', 
            'regiao':'Região', 'marca':'Marca'}
            
    filtros_ui = []
    if 'uf' in filtros_keys:
        filtros_ui.append('<div class="f-group"><label class="f-lbl">UF da Loja</label><select class="f-select filter-select" id="f-uf" onchange="changeUF()"><option value="">Todas</option></select></div>')
    if 'cidade' in filtros_keys:
        filtros_ui.append('<div class="f-group"><label class="f-lbl">Cidade da Loja</label><select class="f-select filter-select" id="f-cidade" onchange="aplicarFiltro()"><option value="">Todas</option></select></div>')
        
    for k in filtros_keys:
        if k in ['uf', 'cidade']: continue
        filtros_ui.append(f'<div class="f-group"><label class="f-lbl">{lbls.get(k, k)}</label><select class="f-select filter-select" id="f-{k}" onchange="aplicarFiltro()"><option value="">Todos</option></select></div>')

    filtros_html = "\\n".join(filtros_ui)

    html = f'''<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Dashboard CRM</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap" rel="stylesheet"/>
  <script src="data.js"></script>
  <style>
    :root {{--blue:#006EFF;--bg:#F0F4F8;--card:#FFF;--border:#E5E7EB;--t1:#111827;--t2:#6B7280;--t3:#9CA3AF;--sidebar:80px;}}
    *{{box-sizing:border-box;margin:0;padding:0;font-family:'Inter',sans-serif;}}
    body{{background:var(--bg);color:var(--t1);display:flex;}}
    .sidebar{{width:var(--sidebar);height:100vh;background:var(--blue);position:fixed;display:flex;flex-direction:column;align-items:center;padding:24px 0;z-index:10;}}
    .logo-img{{width:45px;margin-bottom:30px;}}
    .nav-item{{width:48px;height:48px;border-radius:12px;display:flex;align-items:center;justify-content:center;color:rgba(255,255,255,.5);text-decoration:none;margin-bottom:10px;}}
    .nav-item.active{{background:rgba(255,255,255,.2);color:#fff;}}
    .nav-item svg{{width:24px;height:24px;stroke:currentColor;fill:none;stroke-width:2;stroke-linecap:round;stroke-linejoin:round;}}
    .main{{margin-left:var(--sidebar);padding:40px;flex:1;}}
    h1{{font-size:26px;font-weight:800;}}
    .sub{{font-size:13px;color:var(--t3);margin-top:5px;}}
    
    .filter-bar {{background:#fff;border:1px solid var(--border);border-radius:12px;padding:20px;margin:30px 0;display:flex;flex-wrap:wrap;gap:20px;align-items:flex-end;}}
    .f-group {{display:flex;flex-direction:column;gap:5px;min-width:140px;}}
    .f-lbl {{font-size:11px;font-weight:700;color:var(--t2);text-transform:uppercase;}}
    .f-select {{font-size:13px;padding:8px 30px 8px 12px;border:1px solid var(--border);border-radius:6px;background:#fff;cursor:pointer;outline:none;}}
    .btn {{padding:9px 16px;border-radius:6px;border:1px solid var(--blue);background:none;color:var(--blue);font-weight:600;cursor:pointer;font-size:12px;}}
    .btn:hover {{background:var(--blue);color:#fff;}}

    .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:24px;}}
    .card{{background:var(--card);border:1px solid var(--border);border-radius:16px;padding:24px;box-shadow:0 2px 10px rgba(0,0,0,.03);}}
    .card-label{{font-size:12px;font-weight:700;color:var(--t2);text-transform:uppercase;margin-bottom:12px;}}
    .card-val{{font-size:32px;font-weight:800;margin-bottom:8px;}}
    .card-desc{{font-size:13px;color:var(--t3);}}
  </style>
</head>
<body>
  <aside class="sidebar">
    <img src="Logotipo em branco.svg" class="logo-img">
    <a href="index.html" class="nav-item"><svg viewBox="0 0 24 24"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg></a>
    <a href="#" class="nav-item active"><svg viewBox="0 0 24 24"><circle cx="9" cy="7" r="4"/><path d="M3 21v-2a4 4 0 0 1 4-4h4a4 4 0 0 1 4 4v2"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/><path d="M21 21v-2a4 4 0 0 0-3-3.87"/></svg></a>
  </aside>
  <main class="main">
    <div>
      <h1>Performance Clientes CRM</h1>
      <p class="sub" id="lbl-update"></p>
    </div>
    
    <div class="filter-bar">
      {filtros_html}
      <button class="btn" onclick="limpar()">Limpar</button>
      <div style="font-size:11px;color:#9CA3AF;max-width:200px;margin-left:auto;">
        * Nota: A visualização offline atualiza um filtro analítico por vez. Cidades mapeadas pelo local de compra (loja).
      </div>
    </div>

    <div class="grid">
      <div class="card"><div class="card-label">Clientes Totais</div><div class="card-val" id="v-total"></div><div class="card-desc">Total na base DATAMART_CLIENTES</div></div>
      <div class="card"><div class="card-label">Identificados</div><div class="card-val" id="v-ident"></div><div class="card-desc">Primeira compra preenchida</div></div>
      <div class="card"><div class="card-label">Ativos (90 dias)</div><div class="card-val" id="v-ativos" style="color:var(--green)"></div><div class="card-desc">Compraram nos últimos 90 dias</div></div>
      <div class="card"><div class="card-label">Novos (30 dias)</div><div class="card-val" id="v-novos" style="color:var(--blue)"></div><div class="card-desc">Primeira compra recente</div></div>
      <div class="card"><div class="card-label">LTV Médio</div><div class="card-val" id="v-ltv" style="color:var(--orange)"></div><div class="card-desc">Receita média total por cliente</div></div>
      <div class="card"><div class="card-label">Ticket Médio</div><div class="card-val" id="v-ticket" style="color:var(--purple)"></div><div class="card-desc">Valor por transação (Valor / Compras)</div></div>
    </div>
  </main>

  <script>
    // Inicialização
    document.getElementById('lbl-update').textContent = 'Última atualização: ' + LAST_UPDATE;
    
    // Popular Dropdowns genéricos
    for(let k in OPCOES) {{
      const el = document.getElementById('f-' + k);
      if(el && k !== 'cidade') {{
        OPCOES[k].forEach(opt => {{
          el.add(new Option(opt, opt));
        }});
      }}
    }}

    // Cascata UF -> Cidade
    function changeUF() {{
      const uf = document.getElementById('f-uf').value;
      const elCid = document.getElementById('f-cidade');
      if(!elCid) return;
      
      // Limpa cidades
      while(elCid.options.length > 1) elCid.remove(1);
      
      if(uf && UF_CIDADE[uf]) {{
        UF_CIDADE[uf].forEach(c => elCid.add(new Option(c, c)));
      }}
      aplicarFiltro();
    }}

    function fmtN(v) {{ return v.toLocaleString('pt-BR'); }}
    function fmtB(v) {{ return v.toLocaleString('pt-BR', {{style:'currency',currency:'BRL'}}); }}

    function setVals(d) {{
      document.getElementById('v-total').textContent = fmtN(d.total || 0);
      document.getElementById('v-ident').textContent = fmtN(d.identificados || 0);
      document.getElementById('v-ativos').textContent = fmtN(d.ativos || 0);
      document.getElementById('v-novos').textContent = fmtN(d.novos || 0);
      document.getElementById('v-ltv').textContent = fmtB(d.ltv || 0);
      document.getElementById('v-ticket').textContent = fmtB(d.ticket || 0);
    }}

    function aplicarFiltro() {{
      // Prioridade de busca
      const selects = document.querySelectorAll('.filter-select');
      let ativo = null, val = null;
      
      // Checa qual dropdown está selecionado (pegando o mais específico)
      for(let s of selects) {{
        if(s.value) {{
          ativo = s.id.replace('f-','');
          val = s.value;
          // se selecionou cidade, usa ela. se tem uf e cidade, a cidade sobrescreve.
          if(ativo === 'cidade') break; 
        }}
      }}

      if(!ativo) {{
        setVals(TOTAIS);
        return;
      }}

      const rows = DADOS[ativo] || [];
      const match = rows.find(r => r.dim === val);
      setVals(match || {{}});
    }}

    function limpar() {{
      document.querySelectorAll('.filter-select').forEach(s => s.value='');
      changeUF(); // reseta lista de cidades
      setVals(TOTAIS);
    }}

    // Carrega iniciais
    setVals(TOTAIS);
  </script>
</body>
</html>'''
    with open(f'{OUT}/crm.html', 'w', encoding='utf-8') as f:
        f.write(html)

if __name__ == "__main__":
    run()
