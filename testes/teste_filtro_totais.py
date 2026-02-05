import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO

# Simular o HTML com linhas de totais
html_exemplo = """
<table>
<tr>
    <th>Região</th>
    <th>Código UF</th>
    <th>UF</th>
    <th>Código IBGE</th>
    <th>Município</th>
    <th>Total</th>
</tr>
<tr>
    <td>NORDESTE</td>
    <td>26</td>
    <td>PE</td>
    <td>260010</td>
    <td>AFOGADOS DA INGAZEIRA</td>
    <td>26.0</td>
</tr>
<tr>
    <td>TOTAL ESTADO PERNAMBUCO</td>
    <td>TOTAL ESTADO PERNAMBUCO</td>
    <td>TOTAL ESTADO PERNAMBUCO</td>
    <td>TOTAL ESTADO PERNAMBUCO</td>
    <td>TOTAL ESTADO PERNAMBUCO</td>
    <td>792.0</td>
</tr>
<tr>
    <td>TOTAL REGIÃO NORDESTE</td>
    <td>TOTAL REGIÃO NORDESTE</td>
    <td>TOTAL REGIÃO NORDESTE</td>
    <td>TOTAL REGIÃO NORDESTE</td>
    <td>TOTAL REGIÃO NORDESTE</td>
    <td>3193.0</td>
</tr>
<tr>
    <td>TOTAL BRASIL</td>
    <td>TOTAL BRASIL</td>
    <td>TOTAL BRASIL</td>
    <td>TOTAL BRASIL</td>
    <td>TOTAL BRASIL</td>
    <td>47843.0</td>
</tr>
</table>
"""

def limpar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Limpa e renomeia as colunas do DataFrame"""
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
    
    # Ajustar nomes específicos
    if len(novas_colunas) >= 5:
        novas_colunas[0] = "Regiao"
        novas_colunas[1] = "Codigo_UF"
        novas_colunas[2] = "UF"
        novas_colunas[3] = "Codigo_IBGE"
        novas_colunas[4] = "Municipio"
        if len(novas_colunas) >= 6:
            novas_colunas[5] = "Total"
    
    df.columns = novas_colunas
    return df

# Processar HTML
soup = BeautifulSoup(html_exemplo, 'html.parser')
tables = soup.find_all('table')

if tables:
    df = pd.read_html(StringIO(str(tables[0])))[0]
    df = df.dropna(how='all')
    df = limpar_colunas(df)
    
    print("=" * 80)
    print("ANTES DO FILTRO:")
    print("=" * 80)
    print(df[['Municipio', 'Total']].to_string())
    print(f"\nTotal de linhas: {len(df)}")
    
    # Aplicar filtro (remover linhas de totais)
    if 'Municipio' in df.columns:
        df_filtrado = df[
            ~df['Municipio'].astype(str).str.contains('TOTAL', case=False, na=False)
        ]
        
        print("\n" + "=" * 80)
        print("DEPOIS DO FILTRO:")
        print("=" * 80)
        print(df_filtrado[['Municipio', 'Total']].to_string())
        print(f"\nTotal de linhas: {len(df_filtrado)}")
        
        # Verificar se Afogados da Ingazeira está presente
        afogados = df_filtrado[df_filtrado['Municipio'].astype(str).str.contains('INGAZEIRA', case=False, na=False)]
        if not afogados.empty:
            print(f"\nOK - AFOGADOS DA INGAZEIRA encontrado com Total = {afogados.iloc[0]['Total']}")
        else:
            print("\nERRO - AFOGADOS DA INGAZEIRA NAO encontrado!")
        
        # Verificar se linhas de totais foram removidas
        totais = df_filtrado[df_filtrado['Municipio'].astype(str).str.contains('TOTAL', case=False, na=False)]
        if totais.empty:
            print("OK - Todas as linhas de totais foram removidas corretamente!")
        else:
            print(f"ERRO - Ainda existem {len(totais)} linhas de totais!")

print("\n" + "=" * 80)

