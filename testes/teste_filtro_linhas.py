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
    "coMunicipioIbge": "99",
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
print("TESTE - VERIFICANDO FILTROS E LINHAS COM TOTAL = 0")
print("=" * 80)

response = session.post(url_post, data=data, headers=headers, timeout=None)

if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all('table')
    
    if tables:
        df = pd.read_html(StringIO(str(tables[0])))[0]
        
        print(f"\nTotal de linhas antes de qualquer filtro: {len(df)}")
        
        # Aplicar a função limpar_colunas
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
        
        # Limpar dados (como no código original)
        df = df.dropna(how='all')
        df = limpar_colunas(df)
        
        print(f"Total de linhas após dropna(how='all'): {len(df)}")
        
        # Procurar Afogados da Ingazeira
        print(f"\n{'='*80}")
        print("PROCURANDO AFOGADOS DA INGAZEIRA:")
        print(f"{'='*80}")
        
        afogados = df[df['Municipio'].astype(str).str.contains('INGAZEIRA', case=False, na=False) & 
                     df['Municipio'].astype(str).str.contains('AFOGADOS', case=False, na=False)]
        
        if not afogados.empty:
            print(f"\nEncontrado {len(afogados)} linha(s):")
            print(afogados.to_string())
            print(f"\nValor da coluna Total: {afogados.iloc[0]['Total']}")
            print(f"Tipo: {type(afogados.iloc[0]['Total'])}")
        else:
            print("\nNAO ENCONTRADO!")
        
        # Verificar linhas com Total = 0
        print(f"\n{'='*80}")
        print("LINHAS COM TOTAL = 0:")
        print(f"{'='*80}")
        total_zero = df[df['Total'] == 0]
        print(f"Total de linhas com Total = 0: {len(total_zero)}")
        if len(total_zero) > 0:
            print("\nPrimeiras 5 linhas:")
            print(total_zero.head().to_string())
        
        # Verificar linhas com Total vazio ou NaN
        print(f"\n{'='*80}")
        print("LINHAS COM TOTAL VAZIO OU NaN:")
        print(f"{'='*80}")
        total_vazio = df[df['Total'].isna() | (df['Total'] == '')]
        print(f"Total de linhas: {len(total_vazio)}")
        
        # Verificar linhas que contêm "TOTAL" no nome do município (linhas de totais)
        print(f"\n{'='*80}")
        print("LINHAS DE TOTAIS (que contém 'TOTAL' no município):")
        print(f"{'='*80}")
        linhas_totais = df[df['Municipio'].astype(str).str.contains('TOTAL', case=False, na=False)]
        print(f"Total de linhas: {len(linhas_totais)}")
        if len(linhas_totais) > 0:
            print("\nLinhas de totais:")
            print(linhas_totais[['Municipio', 'Total']].to_string())

print("\n" + "=" * 80)
print("TESTE CONCLUIDO")
print("=" * 80)

