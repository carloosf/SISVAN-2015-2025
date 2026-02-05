import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import os

# Testar com HTML que tem dados
html_file = "resposta_sisvan_2025_anual_sexoM_raca01_Entre_2_anos_a_5_anos.html"

if os.path.exists(html_file):
    with open(html_file, "r", encoding="utf-8") as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    
    if tables:
        print("=" * 80)
        print("ESTRUTURA DA TABELA - COM DADOS")
        print("=" * 80)
        
        df = pd.read_html(StringIO(str(tables[0])))[0]
        
        print(f"\nShape: {df.shape}")
        print(f"\nColunas originais ({len(df.columns)}):")
        for i, col in enumerate(df.columns):
            print(f"  [{i}] {col} (tipo: {type(col)})")
        
        print(f"\nPrimeiras 3 linhas:")
        print(df.head(3).to_string())
        
        print(f"\nValores únicos na primeira coluna:")
        if len(df.columns) > 0:
            print(df.iloc[:, 0].unique()[:10])
else:
    print(f"Arquivo {html_file} não encontrado")

# Testar com HTML sem dados
html_file2 = "resposta_sisvan_2025_anual_sexoM_raca01_Menor_de_6_meses.html"

if os.path.exists(html_file2):
    with open(html_file2, "r", encoding="utf-8") as f:
        html_content2 = f.read()
    
    soup2 = BeautifulSoup(html_content2, 'html.parser')
    tables2 = soup2.find_all('table')
    
    if tables2:
        print("\n\n" + "=" * 80)
        print("ESTRUTURA DA TABELA - SEM DADOS (APENAS TOTAIS)")
        print("=" * 80)
        
        df2 = pd.read_html(StringIO(str(tables2[0])))[0]
        
        print(f"\nShape: {df2.shape}")
        print(f"\nColunas originais ({len(df2.columns)}):")
        for i, col in enumerate(df2.columns):
            print(f"  [{i}] {col} (tipo: {type(col)})")
        
        print(f"\nPrimeiras 3 linhas:")
        print(df2.head(3).to_string())
        
        print(f"\nValores únicos na primeira coluna:")
        if len(df2.columns) > 0:
            print(df2.iloc[:, 0].unique()[:10])

print("\n" + "=" * 80)

