import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from io import StringIO
import json

# --- CONFIGURAÇÕES DE ACESSO ---
url_index = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index"
url_post = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/estadonutricional"

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index"
}

# --- PARÂMETROS DA CONSULTA POST ---
data = {
    "tpRelatorio": "2",
    "coVisualizacao": "1",
    "nuAno": "2024",
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

# --- CONSULTA POST E EXIBIÇÃO DE RESULTADOS ---
print("=" * 80)
print("FAZENDO CONSULTA POST AO SISVAN...")
print("=" * 80)
print(f"\nURL: {url_post}")
print(f"\nParâmetros da consulta:")
for key, value in data.items():
    print(f"  {key}: {value}")

try:
    # Primeiro, fazer uma requisição GET para obter a sessão/cookies
    print("\nObtendo sessão do servidor...")
    session = requests.Session()
    session.get(url_index, headers=headers, timeout=None)
    
    # Fazer a requisição POST sem timeout (aguardar o tempo necessário)
    print("\n" + "-" * 80)
    print("Enviando requisição POST...")
    print("⏳ Aguardando resposta do servidor (pode demorar alguns minutos)...")
    print("-" * 80)
    
    import datetime
    inicio = datetime.datetime.now()
    print(f"⏰ Início: {inicio.strftime('%H:%M:%S')}")
    
    response = session.post(url_post, data=data, headers=headers, timeout=None)
    
    fim = datetime.datetime.now()
    tempo_decorrido = (fim - inicio).total_seconds()
    print(f"✅ Resposta recebida! Tempo decorrido: {tempo_decorrido:.2f} segundos ({tempo_decorrido/60:.2f} minutos)")
    
    print(f"\nStatus Code: {response.status_code}")
    print(f"Content-Type: {response.headers.get('Content-Type', 'N/A')}")
    
    if response.status_code == 200:
        print("\n" + "=" * 80)
        print("RESULTADO DA CONSULTA - DADOS SISVAN")
        print("=" * 80)
        
        # Tentar parsear como HTML primeiro
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Procurar por tabelas na resposta
        tables = soup.find_all('table')
        if tables:
            print(f"\nTabelas encontradas: {len(tables)}")
            df_final = None
            
            for i, table in enumerate(tables, 1):
                print(f"\n--- TABELA {i} ---")
                df_table = pd.read_html(StringIO(str(table)))[0]
                print(df_table.to_string())
                
                # Processar e limpar os dados da tabela
                if df_final is None:
                    df_final = df_table.copy()
                else:
                    # Se houver múltiplas tabelas, concatenar
                    df_final = pd.concat([df_final, df_table], ignore_index=True)
            
            # Limpar e estruturar o DataFrame para CSV
            if df_final is not None:
                # Remover linhas totalmente vazias
                df_final = df_final.dropna(how='all')
                
                # Renomear colunas de forma mais clara
                novas_colunas = []
                for col in df_final.columns:
                    col_str = str(col)
                    # Se for uma tupla (coluna multi-nível), pegar o último elemento útil
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
                
                # Ajustar nomes específicos baseado na posição
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
                
                # Remover linhas de totais se necessário (opcional - você pode querer mantê-las)
                # df_final = df_final[~df_final.iloc[:, 0].str.contains('TOTAL', case=False, na=False)]
                
                # Nome do arquivo CSV baseado nos parâmetros da consulta
                csv_filename = f"sisvan_crianca_{data['nuAno']}_mes{data['nuMes[]']}_uf{data['coUfIbge']}_sexo{data['ds_sexo2']}.csv"
                
                # Salvar em CSV com separador ponto-e-vírgula
                df_final.to_csv(csv_filename, index=False, encoding='utf-8-sig', sep=';')
                
                print("\n" + "=" * 80)
                print("✅ ARQUIVO CSV CRIADO COM SUCESSO!")
                print("=" * 80)
                print(f"\n📁 Arquivo salvo: {csv_filename}")
                print(f"📊 Total de registros: {len(df_final)}")
                print(f"📋 Total de colunas: {len(df_final.columns)}")
                print(f"\nColunas no CSV:")
                for idx, col in enumerate(df_final.columns, 1):
                    print(f"  {idx}. {col}")
                print("\n" + "=" * 80)
        else:
            # Tentar parsear como CSV se não houver tabelas HTML
            try:
                df = pd.read_csv(StringIO(response.text), encoding='utf-8-sig')
                print(f"\nTotal de registros encontrados: {len(df)}")
                print(f"\nColunas disponíveis: {list(df.columns)}")
                print("\n" + "-" * 80)
                print("Primeiros registros:")
                print("-" * 80)
                print(df.head().to_string())
                print("\n" + "-" * 80)
                print("Resumo estatístico:")
                print("-" * 80)
                print(df.describe().to_string())
                
                # Salvar em CSV
                csv_filename = f"sisvan_crianca_{data['nuAno']}_mes{data['nuMes[]']}_uf{data['coUfIbge']}_sexo{data['ds_sexo2']}.csv"
                df.to_csv(csv_filename, index=False, encoding='utf-8-sig', sep=';')
                
                print("\n" + "=" * 80)
                print("✅ ARQUIVO CSV CRIADO COM SUCESSO!")
                print("=" * 80)
                print(f"\n📁 Arquivo salvo: {csv_filename}")
                print(f"📊 Total de registros: {len(df)}")
                print("=" * 80)
            except:
                # Se não for CSV, mostrar parte do conteúdo HTML/texto
                print("\nConteúdo da resposta (primeiros 2000 caracteres):")
                print("-" * 80)
                print(response.text[:2000])
                print("\n...")
        
        print("\n" + "=" * 80)
        
        # Salvar resposta em arquivo para análise
        with open("resposta_sisvan.html", "w", encoding="utf-8") as f:
            f.write(response.text)
        print("\nResposta salva em: resposta_sisvan.html")
        
    else:
        print(f"\nErro na requisição. Status: {response.status_code}")
        print(f"Resposta: {response.text[:500]}")
        
except requests.exceptions.RequestException as e:
    print(f"\nErro na requisição: {e}")
    print(f"\nDetalhes: {type(e).__name__}")
except Exception as e:
    print(f"\nErro inesperado: {e}")
    import traceback
    traceback.print_exc()

# Mostrar resumo mesmo em caso de erro
print("\n" + "=" * 80)
print("RESUMO DA CONSULTA")
print("=" * 80)
print(f"\n✅ Parâmetros configurados com sucesso")
print(f"✅ Código de requisição POST implementado")
print(f"\n📋 Parâmetros da consulta:")
print(f"   - Ano: {data['nuAno']}")
print(f"   - Mês: {data['nuMes[]']}")
print(f"   - UF: {data['coUfIbge']} (Pernambuco)")
print(f"   - Município: {data['coMunicipioIbge']}")
print(f"   - Ciclo de Vida: {data['nu_ciclo_vida']} (Criança)")
print(f"   - Idade: {data['nu_idade_inicio']} a {data['nu_idade_fim']} anos")
print(f"   - Sexo: {data['ds_sexo2']} (Masculino)")
print(f"   - Raça/Cor: {data['ds_raca_cor2']}")
print("=" * 80)

