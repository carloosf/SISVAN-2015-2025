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

# Consulta geral (todos os municípios)
data = {
    "tpRelatorio": "2",
    "coVisualizacao": "1",
    "nuAno": "2025",
    "nuMes[]": "01",
    "tpFiltro": "M",
    "coRegiao": "",
    "coUfIbge": "26",
    "coMunicipioIbge": "99",  # TODOS
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

print("=" * 80)
print("TESTE DETALHADO - VERIFICANDO COLUNA TOTAL")
print("=" * 80)

response = session.post(url_post, data=data, headers=headers, timeout=None)

if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all('table')
    
    if tables:
        df = pd.read_html(StringIO(str(tables[0])))[0]
        
        print(f"\nDimensoes: {df.shape[0]} linhas x {df.shape[1]} colunas")
        print(f"\nColunas originais:")
        for i, col in enumerate(df.columns):
            print(f"  [{i}] {col}")
        
        # Procurar Afogados da Ingazeira
        print(f"\n{'='*80}")
        print("LINHA DE AFOGADOS DA INGAZEIRA:")
        print(f"{'='*80}")
        
        for col in df.columns:
            if df[col].dtype == 'object':
                matches = df[df[col].astype(str).str.contains('INGAZEIRA', case=False, na=False)]
                if not matches.empty and 'AFOGADOS' in matches.iloc[0, matches.columns.get_loc(col)]:
                    linha_idx = matches.index[0]
                    print(f"\nIndice da linha: {linha_idx}")
                    print(f"\nTodos os valores da linha:")
                    for i, val in enumerate(df.iloc[linha_idx]):
                        col_name = df.columns[i]
                        print(f"  Coluna [{i}] '{col_name}': {val} (tipo: {type(val).__name__})")
                    
                    # Verificar especificamente a última coluna (Total)
                    print(f"\n{'='*80}")
                    print("VERIFICACAO DA COLUNA TOTAL:")
                    print(f"{'='*80}")
                    ultima_col = df.columns[-1]
                    print(f"Ultima coluna: '{ultima_col}'")
                    print(f"Valor na linha de Afogados: {df.iloc[linha_idx, -1]}")
                    print(f"Tipo: {type(df.iloc[linha_idx, -1])}")
                    
                    # Verificar se há outras colunas com "Total" no nome
                    print(f"\nColunas que contem 'Total':")
                    for i, col in enumerate(df.columns):
                        if 'Total' in str(col):
                            print(f"  [{i}] '{col}' = {df.iloc[linha_idx, i]}")
                    
                    break

print("\n" + "=" * 80)
print("TESTE CONCLUIDO")
print("=" * 80)

