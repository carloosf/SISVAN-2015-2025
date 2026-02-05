import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import os

# Arquivo HTML salvo
html_file = "resposta_sisvan.html"

if os.path.exists(html_file):
    print("=" * 80)
    print("PROCESSANDO ARQUIVO HTML PARA CSV")
    print("=" * 80)
    
    # Ler o arquivo HTML
    with open(html_file, "r", encoding="utf-8") as f:
        html_content = f.read()
    
    # Parsear HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Procurar por tabelas
    tables = soup.find_all('table')
    
    if tables:
        print(f"\nTabelas encontradas: {len(tables)}")
        df_final = None
        
        for i, table in enumerate(tables, 1):
            print(f"\nProcessando tabela {i}...")
            try:
                df_table = pd.read_html(StringIO(str(table)))[0]
                
                if df_final is None:
                    df_final = df_table.copy()
                else:
                    df_final = pd.concat([df_final, df_table], ignore_index=True)
                    
                print(f"  OK - Tabela {i} processada: {len(df_table)} linhas")
            except Exception as e:
                print(f"  ERRO ao processar tabela {i}: {e}")
        
        if df_final is not None:
            # Limpar dados
            df_final = df_final.dropna(how='all')
            
            # Renomear colunas de forma mais clara
            # Extrair apenas o último nível dos nomes de colunas multi-nível
            novas_colunas = []
            for col in df_final.columns:
                col_str = str(col)
                # Se for uma tupla (coluna multi-nível), pegar o último elemento útil
                if isinstance(col, tuple):
                    # Pegar o último elemento não vazio e não "Unnamed"
                    partes = [p for p in col if p and 'Unnamed' not in str(p)]
                    if partes:
                        col_str = partes[-1]
                    else:
                        col_str = str(col[-1])
                
                # Limpar e padronizar nomes
                col_str = col_str.strip()
                # Remover prefixos comuns
                if 'PESO X IDADE' in col_str:
                    col_str = col_str.replace('PESO X IDADE', '').strip()
                if col_str.startswith(','):
                    col_str = col_str[1:].strip()
                
                novas_colunas.append(col_str if col_str else f"Coluna_{len(novas_colunas)+1}")
            
            # Ajustar nomes específicos baseado na posição (conhecendo a estrutura da tabela)
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
            
            df_final.columns = novas_colunas
            
            # Nome do arquivo CSV
            csv_filename = "sisvan_crianca_2024_mes01_uf26_sexoM.csv"
            
            # Salvar em CSV
            df_final.to_csv(csv_filename, index=False, encoding='utf-8-sig', sep=';')
            
            print("\n" + "=" * 80)
            print("ARQUIVO CSV CRIADO COM SUCESSO!")
            print("=" * 80)
            print(f"\nArquivo salvo: {csv_filename}")
            print(f"Total de registros: {len(df_final)}")
            print(f"Total de colunas: {len(df_final.columns)}")
            print(f"\nPrimeiras linhas do CSV:")
            print("-" * 80)
            print(df_final.head(10).to_string())
            print("\n" + "=" * 80)
        else:
            print("\nERRO: Nenhuma tabela foi processada com sucesso.")
    else:
        print("\nERRO: Nenhuma tabela encontrada no HTML.")
else:
    print(f"ERRO: Arquivo {html_file} nao encontrado.")

