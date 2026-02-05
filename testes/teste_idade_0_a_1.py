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

# Testar com a faixa etária EXATA do payload do usuário
print("=" * 80)
print("TESTE COM FAIXA ETARIA EXATA DO PAYLOAD")
print("=" * 80)
print("Idade: 0 a 1 (nu_idade_inicio=0, nu_idade_fim=1)")
print("=" * 80)

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
    "nu_idade_inicio": "0",  # EXATO como no payload
    "nu_idade_fim": "1",     # EXATO como no payload
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

print("\nFazendo consulta...")
response = session.post(url_post, data=data, headers=headers, timeout=None)

if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all('table')
    
    if tables:
        df = pd.read_html(StringIO(str(tables[0])))[0]
        
        # Aplicar limpar_colunas
        def limpar_colunas(df: pd.DataFrame) -> pd.DataFrame:
            novas_colunas = []
            for col in df.columns:
                col_str = str(col)
                if isinstance(col, tuple):
                    partes = [p for p in col if p and 'Unnamed' not in str(p)]
                    if partes:
                        col_str = partes[-1]
                    else:
                        col_str = str(col[-1])
                
                col_str = col_str.strip()
                if 'PESO X IDADE' in col_str:
                    col_str = col_str.replace('PESO X IDADE', '').strip()
                if col_str.startswith(','):
                    col_str = col_str[1:].strip()
                
                novas_colunas.append(col_str if col_str else f"Coluna_{len(novas_colunas)+1}")
            
            if len(novas_colunas) >= 14:
                novas_colunas[0] = "Regiao"
                novas_colunas[1] = "Codigo_UF"
                novas_colunas[2] = "UF"
                novas_colunas[3] = "Codigo_IBGE"
                novas_colunas[4] = "Municipio"
                novas_colunas[5] = "MuitoBaixo_Qtd"
                novas_colunas[6] = "MuitoBaixo_Perc"
                novas_colunas[7] = "Baixo_Qtd"
                novas_colunas[8] = "Baixo_Perc"
                novas_colunas[9] = "Adequado_Qtd"
                novas_colunas[10] = "Adequado_Perc"
                novas_colunas[11] = "Elevado_Qtd"
                novas_colunas[12] = "Elevado_Perc"
                novas_colunas[13] = "Total"
            
            df.columns = novas_colunas
            return df
        
        df = df.dropna(how='all')
        df = limpar_colunas(df)
        
        # Procurar Afogados da Ingazeira
        afogados = df[df['Municipio'].astype(str).str.contains('INGAZEIRA', case=False, na=False) & 
                     df['Municipio'].astype(str).str.contains('AFOGADOS', case=False, na=False)]
        
        if not afogados.empty:
            print(f"\n{'='*80}")
            print("AFOGADOS DA INGAZEIRA ENCONTRADO:")
            print(f"{'='*80}")
            print(afogados[['Municipio', 'Codigo_IBGE', 'Total']].to_string())
            print(f"\nValor Total: {afogados.iloc[0]['Total']}")
        else:
            print("\nNAO ENCONTRADO!")

print("\n" + "=" * 80)
print("TESTE CONCLUIDO")
print("=" * 80)

