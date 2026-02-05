import pandas as pd
from io import StringIO

# Simular a estrutura da tabela
html_simulado = """
<table>
<tr>
    <th>Região</th>
    <th>Código UF</th>
    <th>UF</th>
    <th>Código IBGE</th>
    <th>Município</th>
    <th>Muito Baixo Qtd</th>
    <th>Muito Baixo %</th>
    <th>Baixo Qtd</th>
    <th>Baixo %</th>
    <th>Adequado Qtd</th>
    <th>Adequado %</th>
    <th>Elevado Qtd</th>
    <th>Elevado %</th>
    <th>Total</th>
</tr>
<tr>
    <td>NORDESTE</td>
    <td>26</td>
    <td>PE</td>
    <td>260010</td>
    <td>AFOGADOS DA INGAZEIRA</td>
    <td>0</td>
    <td>-</td>
    <td>0.0</td>
    <td>-</td>
    <td>26.0</td>
    <td>100%</td>
    <td>0.0</td>
    <td>-</td>
    <td>26.0</td>
</tr>
</table>
"""

# Ler como o pandas leria
df = pd.read_html(StringIO(html_simulado))[0]

print("DataFrame original:")
print(df)
print(f"\nColunas: {list(df.columns)}")
print(f"\nValores da linha:")
for i, val in enumerate(df.iloc[0]):
    print(f"  [{i}] {val}")

# Simular a função limpar_colunas
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

df_limpo = limpar_colunas(df.copy())
print(f"\n\nDataFrame após limpar_colunas:")
print(df_limpo)
print(f"\nColunas: {list(df_limpo.columns)}")
print(f"\nValor da coluna Total: {df_limpo.iloc[0]['Total']}")

