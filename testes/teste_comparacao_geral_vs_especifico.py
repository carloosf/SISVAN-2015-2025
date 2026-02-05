import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO

# --- CONFIGURAÇÕES DE ACESSO ---
url_index = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index"
url_post = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/estadonutricional"

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index"
}

# Criar sessão
session = requests.Session()
session.get(url_index, headers=headers, timeout=15)

# --- TESTE 1: CONSULTA GERAL (TODOS OS MUNICÍPIOS) ---
print("=" * 80)
print("TESTE 1: CONSULTA GERAL (coMunicipioIbge=99 - TODOS OS MUNICÍPIOS)")
print("=" * 80)

data_geral = {
    "tpRelatorio": "2",
    "coVisualizacao": "1",
    "nuAno": "2025",
    "nuMes[]": "01",
    "tpFiltro": "M",
    "coRegiao": "",
    "coUfIbge": "26",
    "coMunicipioIbge": "99",  # TODOS OS MUNICÍPIOS
    "noRegional": "",
    "st_cobertura": "99",
    "nu_ciclo_vida": "1",
    "nu_idade_inicio": "0",
    "nu_idade_fim": "1",
    "nu_indice_cri": "1",
    "nu_indice_ado": "1",
    "nu_idade_ges": "99",
    "ds_sexo2": "M",
    "ds_raca_cor2": "01",
    "co_sistema_origem": "0",
    "CO_POVO_COMUNIDADE": "TODOS",
    "CO_ESCOLARIDADE": "TODOS",
    "verTela": ""
}

print("\nFazendo consulta geral...")
response_geral = session.post(url_post, data=data_geral, headers=headers, timeout=None)

if response_geral.status_code == 200:
    soup_geral = BeautifulSoup(response_geral.text, 'html.parser')
    tables_geral = soup_geral.find_all('table')
    
    if tables_geral:
        df_geral = pd.read_html(StringIO(str(tables_geral[0])))[0]
        
        # Procurar Afogados da Ingazeira
        print("\nProcurando 'AFOGADOS DA INGAZEIRA' na consulta geral...")
        for col in df_geral.columns:
            if df_geral[col].dtype == 'object':
                matches = df_geral[df_geral[col].astype(str).str.contains('INGAZEIRA', case=False, na=False)]
                if not matches.empty:
                    print(f"\nENCONTRADO na coluna '{col}':")
                    print(matches.to_string())
                    print(f"\nValor do Total (última coluna):")
                    print(matches.iloc[:, -1].values)
                    
                    # Verificar se há valores 0 ou vazios
                    print(f"\nVerificando valores na linha:")
                    for idx, val in enumerate(matches.iloc[0]):
                        print(f"  Coluna {idx}: {val} (tipo: {type(val)})")

# --- TESTE 2: CONSULTA ESPECÍFICA (AFOGADOS DA INGAZEIRA) ---
print("\n\n" + "=" * 80)
print("TESTE 2: CONSULTA ESPECÍFICA (coMunicipioIbge=260010 - AFOGADOS DA INGAZEIRA)")
print("=" * 80)

data_especifico = {
    "tpRelatorio": "2",
    "coVisualizacao": "1",
    "nuAno": "2025",
    "nuMes[]": "01",
    "tpFiltro": "M",
    "coRegiao": "",
    "coUfIbge": "26",
    "coMunicipioIbge": "260010",  # AFOGADOS DA INGAZEIRA
    "noRegional": "",
    "st_cobertura": "99",
    "nu_ciclo_vida": "1",
    "nu_idade_inicio": "0",
    "nu_idade_fim": "1",
    "nu_indice_cri": "1",
    "nu_indice_ado": "1",
    "nu_idade_ges": "99",
    "ds_sexo2": "M",
    "ds_raca_cor2": "01",
    "co_sistema_origem": "0",
    "CO_POVO_COMUNIDADE": "TODOS",
    "CO_ESCOLARIDADE": "TODOS",
    "verTela": ""
}

print("\nFazendo consulta específica...")
response_especifico = session.post(url_post, data=data_especifico, headers=headers, timeout=None)

if response_especifico.status_code == 200:
    soup_especifico = BeautifulSoup(response_especifico.text, 'html.parser')
    tables_especifico = soup_especifico.find_all('table')
    
    if tables_especifico:
        df_especifico = pd.read_html(StringIO(str(tables_especifico[0])))[0]
        
        # Procurar Afogados da Ingazeira
        print("\nDados da consulta específica:")
        for col in df_especifico.columns:
            if df_especifico[col].dtype == 'object':
                matches = df_especifico[df_especifico[col].astype(str).str.contains('INGAZEIRA', case=False, na=False)]
                if not matches.empty:
                    print(f"\nENCONTRADO na coluna '{col}':")
                    print(matches.to_string())
                    print(f"\nValor do Total (última coluna):")
                    print(matches.iloc[:, -1].values)
                    
                    # Verificar se há valores 0 ou vazios
                    print(f"\nVerificando valores na linha:")
                    for idx, val in enumerate(matches.iloc[0]):
                        print(f"  Coluna {idx}: {val} (tipo: {type(val)})")

print("\n" + "=" * 80)
print("COMPARAÇÃO CONCLUÍDA")
print("=" * 80)

