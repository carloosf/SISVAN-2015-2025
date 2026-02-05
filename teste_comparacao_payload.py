"""
Script para testar o payload e comparar com o HTML existente
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import os

# URLs e headers
url_post = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/estadonutricional"
url_index = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index"

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index"
}

def limpar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia e organiza as colunas do DataFrame"""
    if df.empty:
        return df
    
    # Verificar quantas colunas temos
    num_colunas = len(df.columns)
    
    if num_colunas < 14:
        print(f"  AVISO: Tabela com {num_colunas} colunas (esperado 14)")
        return df
    
    # Renomear colunas
    novas_colunas = list(df.columns)
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


def processar_html_para_dataframe(html_content: str, nome_arquivo: str = ""):
    """Processa HTML e retorna DataFrame limpo apenas com municípios"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')
        
        if not tables:
            print(f"  ERRO [{nome_arquivo}]: Nenhuma tabela encontrada no HTML")
            return None
        
        # Procurar a tabela principal (geralmente a primeira com dados)
        df_final = None
        for table in tables:
            try:
                df_table = pd.read_html(StringIO(str(table)))[0]
                
                # Verificar se tem estrutura de dados (pelo menos algumas colunas)
                if len(df_table.columns) >= 10:
                    if df_final is None:
                        df_final = df_table.copy()
                    else:
                        df_final = pd.concat([df_final, df_table], ignore_index=True)
            except Exception as e:
                continue
        
        if df_final is None or df_final.empty:
            print(f"  ERRO [{nome_arquivo}]: Nenhum dado encontrado nas tabelas")
            return None
        
        # Remover linhas completamente vazias
        df_final = df_final.dropna(how='all')
        
        # Limpar colunas
        df_final = limpar_colunas(df_final)
        
        # Verificar se temos a coluna Municipio
        if 'Municipio' not in df_final.columns:
            print(f"  ERRO [{nome_arquivo}]: Coluna 'Municipio' não encontrada após limpeza")
            return None
        
        # Filtrar apenas municípios (remover totais)
        # Remover linhas onde o nome do município contém "TOTAL"
        df_final = df_final[
            ~df_final['Municipio'].astype(str).str.contains('TOTAL', case=False, na=False)
        ]
        
        # Remover linhas onde o código IBGE não é numérico válido (para garantir que são municípios)
        # Códigos IBGE de municípios são numéricos de 6 dígitos
        if 'Codigo_IBGE' in df_final.columns:
            df_final = df_final[
                df_final['Codigo_IBGE'].astype(str).str.match(r'^\d{6}$', na=False)
            ]
        
        # Resetar índice
        df_final = df_final.reset_index(drop=True)
        
        return df_final
        
    except Exception as e:
        print(f"  ERRO [{nome_arquivo}] ao processar HTML: {e}")
        import traceback
        traceback.print_exc()
        return None


def comparar_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, nome1: str, nome2: str):
    """Compara dois DataFrames e mostra diferenças"""
    print(f"\n{'='*80}")
    print(f"COMPARAÇÃO: {nome1} vs {nome2}")
    print(f"{'='*80}")
    
    print(f"\n{nome1}: {len(df1)} registros")
    print(f"{nome2}: {len(df2)} registros")
    
    if len(df1) != len(df2):
        print(f"\n[DIFERENCA] Numero de registros diferente ({len(df1)} vs {len(df2)})")
    else:
        print(f"\n[OK] Numero de registros igual: {len(df1)}")
    
    # Comparar colunas
    colunas1 = set(df1.columns)
    colunas2 = set(df2.columns)
    
    if colunas1 != colunas2:
        print(f"\n[DIFERENCA] Colunas diferentes")
        print(f"  {nome1} tem: {colunas1 - colunas2}")
        print(f"  {nome2} tem: {colunas2 - colunas1}")
    else:
        print(f"\n[OK] Colunas identicas: {len(colunas1)} colunas")
    
    # Comparar por código IBGE (chave primária)
    if 'Codigo_IBGE' in df1.columns and 'Codigo_IBGE' in df2.columns:
        ibge1 = set(df1['Codigo_IBGE'].astype(str))
        ibge2 = set(df2['Codigo_IBGE'].astype(str))
        
        apenas1 = ibge1 - ibge2
        apenas2 = ibge2 - ibge1
        comum = ibge1 & ibge2
        
        print(f"\nCódigos IBGE:")
        print(f"  Comuns: {len(comum)}")
        print(f"  Apenas em {nome1}: {len(apenas1)}")
        print(f"  Apenas em {nome2}: {len(apenas2)}")
        
        if apenas1:
            print(f"\n  Municípios apenas em {nome1}:")
            for ibge in sorted(list(apenas1)[:10]):  # Mostrar até 10
                municipio = df1[df1['Codigo_IBGE'].astype(str) == ibge]['Municipio'].values
                if len(municipio) > 0:
                    print(f"    - {ibge}: {municipio[0]}")
        
        if apenas2:
            print(f"\n  Municípios apenas em {nome2}:")
            for ibge in sorted(list(apenas2)[:10]):  # Mostrar até 10
                municipio = df2[df2['Codigo_IBGE'].astype(str) == ibge]['Municipio'].values
                if len(municipio) > 0:
                    print(f"    - {ibge}: {municipio[0]}")
        
        # Comparar valores para municípios comuns
        if comum:
            print(f"\nComparando valores para {len(comum)} municípios comuns...")
            diferencas = []
            
            for ibge in sorted(list(comum)[:20]):  # Comparar até 20 municípios
                row1 = df1[df1['Codigo_IBGE'].astype(str) == ibge].iloc[0]
                row2 = df2[df2['Codigo_IBGE'].astype(str) == ibge].iloc[0]
                
                # Comparar colunas numéricas principais
                colunas_numericas = ['Total', 'MuitoBaixo_Qtd', 'Baixo_Qtd', 'Adequado_Qtd', 'Elevado_Qtd']
                for col in colunas_numericas:
                    if col in row1.index and col in row2.index:
                        val1 = pd.to_numeric(row1[col], errors='coerce')
                        val2 = pd.to_numeric(row2[col], errors='coerce')
                        
                        if pd.notna(val1) and pd.notna(val2) and val1 != val2:
                            diferencas.append({
                                'IBGE': ibge,
                                'Municipio': row1['Municipio'],
                                'Coluna': col,
                                nome1: val1,
                                nome2: val2,
                                'Diferenca': abs(val1 - val2)
                            })
            
            if diferencas:
                print(f"\n[DIFERENCA] Encontradas {len(diferencas)} diferencas nos valores:")
                df_diff = pd.DataFrame(diferencas)
                print(df_diff.head(10).to_string(index=False))
            else:
                print(f"\n[OK] Valores identicos para os municipios comparados")
    
    # Comparar totais gerais
    if 'Total' in df1.columns and 'Total' in df2.columns:
        total1 = df1['Total'].astype(str).str.replace(',', '.').astype(float, errors='ignore').sum()
        total2 = df2['Total'].astype(str).str.replace(',', '.').astype(float, errors='ignore').sum()
        
        print(f"\nTotais gerais:")
        print(f"  {nome1}: {total1}")
        print(f"  {nome2}: {total2}")
        
        if abs(total1 - total2) < 0.01:
            print(f"  [OK] Totais identicos")
        else:
            print(f"  [DIFERENCA] Diferenca: {abs(total1 - total2)}")


def main():
    """Função principal"""
    print("=" * 80)
    print("TESTE DE COMPARAÇÃO: Payload vs HTML Existente")
    print("=" * 80)
    
    # 1. Ler HTML existente
    html_file_existente = "SISVAN - Relatórios de Produção.htm"
    
    print(f"\n1. Lendo HTML existente: {html_file_existente}")
    if not os.path.exists(html_file_existente):
        print(f"   ERRO: Arquivo não encontrado!")
        return
    
    try:
        with open(html_file_existente, 'r', encoding='utf-8') as f:
            html_existente = f.read()
        print(f"   OK - Arquivo lido ({len(html_existente)} caracteres)")
    except Exception as e:
        print(f"   ERRO ao ler arquivo: {e}")
        return
    
    # 2. Processar HTML existente
    print(f"\n2. Processando HTML existente...")
    df_existente = processar_html_para_dataframe(html_existente, "HTML Existente")
    
    if df_existente is None or df_existente.empty:
        print("   ERRO: Não foi possível processar HTML existente")
        return
    
    print(f"   OK - {len(df_existente)} municípios encontrados")
    
    # 3. Fazer requisição com o payload
    print(f"\n3. Fazendo requisição com o payload...")
    
    # Decodificar o payload
    payload = {
        "tpRelatorio": "2",
        "coVisualizacao": "1",
        "nuAno": "2025",
        "nuMes[]": "99",  # 99 = TODOS os meses
        "tpFiltro": "M",
        "coRegiao": "",
        "coUfIbge": "26",
        "coMunicipioIbge": "99",  # 99 = Todos os municípios do estado
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
    
    session = requests.Session()
    
    try:
        # Obter sessão inicial
        print("   Obtendo sessão do servidor...")
        session.get(url_index, headers=headers, timeout=15)
        
        # Fazer requisição POST
        print("   Enviando requisição POST...")
        response = session.post(url_post, data=payload, headers=headers, timeout=None)
        
        if response.status_code != 200:
            print(f"   ERRO - Status {response.status_code}")
            return
        
        html_novo = response.text
        print(f"   OK - Resposta recebida ({len(html_novo)} caracteres)")
        
        # Salvar HTML novo para comparação
        html_file_novo = "SISVAN_resposta_teste.html"
        with open(html_file_novo, 'w', encoding='utf-8') as f:
            f.write(html_novo)
        print(f"   HTML salvo em: {html_file_novo}")
        
    except Exception as e:
        print(f"   ERRO na requisição: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 4. Processar HTML novo
    print(f"\n4. Processando HTML novo...")
    df_novo = processar_html_para_dataframe(html_novo, "HTML Novo")
    
    if df_novo is None or df_novo.empty:
        print("   ERRO: Não foi possível processar HTML novo")
        return
    
    print(f"   OK - {len(df_novo)} municípios encontrados")
    
    # 5. Comparar os dois DataFrames
    comparar_dataframes(df_existente, df_novo, "HTML Existente", "HTML Novo")
    
    # 6. Salvar CSVs para análise manual
    print(f"\n5. Salvando CSVs para análise...")
    df_existente.to_csv("comparacao_existente.csv", index=False, encoding='utf-8-sig')
    df_novo.to_csv("comparacao_novo.csv", index=False, encoding='utf-8-sig')
    print(f"   CSVs salvos: comparacao_existente.csv e comparacao_novo.csv")
    
    print(f"\n{'='*80}")
    print("TESTE CONCLUÍDO!")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()

