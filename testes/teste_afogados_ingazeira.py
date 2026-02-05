import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import os

# --- CONFIGURAÇÕES DE ACESSO ---
url_index = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index"
url_post = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/estadonutricional"

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index"
}

# --- PAYLOAD ESPECÍFICO PARA TESTE ---
# Afogados da Ingazeira (260010) - 2025 - Mês 01 - Sexo M - Raça 01 - Idade 0 a 1
data = {
    "tpRelatorio": "2",
    "coVisualizacao": "1",
    "nuAno": "2025",
    "nuMes[]": "01",
    "tpFiltro": "M",
    "coRegiao": "",
    "coUfIbge": "26",
    "coMunicipioIbge": "260010",  # Afogados da Ingazeira
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
print("TESTE - AFOGADOS DA INGAZEIRA")
print("=" * 80)
print(f"\nMunicipio: Afogados da Ingazeira (260010)")
print(f"Ano: 2025")
print(f"Mes: 01")
print(f"Sexo: M (Masculino)")
print(f"Raca: 01 (Branca)")
print(f"Idade: 0 a 1 ano")
print("\n" + "=" * 80)

# Criar sessão
session = requests.Session()
print("\nObtendo sessao do servidor...")
try:
    session.get(url_index, headers=headers, timeout=15)
    print("Sessao obtida com sucesso")
except Exception as e:
    print(f"Erro ao obter sessao: {e}")

# Fazer a consulta
print("\n" + "-" * 80)
print("Fazendo consulta POST...")
print("-" * 80)
print("\nPayload enviado:")
for key, value in data.items():
    print(f"  {key}: {value}")

try:
    response = session.post(url_post, data=data, headers=headers, timeout=None)
    
    print(f"\nStatus Code: {response.status_code}")
    print(f"Content-Type: {response.headers.get('Content-Type', 'N/A')}")
    
    if response.status_code == 200:
        # Salvar HTML completo
        html_filename = "teste_afogados_ingazeira_resposta.html"
        with open(html_filename, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"\nHTML salvo em: {html_filename}")
        
        # Processar HTML
        soup = BeautifulSoup(response.text, 'html.parser')
        tables = soup.find_all('table')
        
        print(f"\nTabelas encontradas: {len(tables)}")
        
        if tables:
            for i, table in enumerate(tables, 1):
                print(f"\n{'='*80}")
                print(f"TABELA {i}")
                print(f"{'='*80}")
                
                try:
                    df = pd.read_html(StringIO(str(table)))[0]
                    print(f"\nDimensoes: {df.shape[0]} linhas x {df.shape[1]} colunas")
                    print(f"\nColunas: {list(df.columns)}")
                    
                    # Procurar por Afogados da Ingazeira
                    print(f"\n{'='*80}")
                    print("PROCURANDO POR 'AFOGADOS DA INGAZEIRA'")
                    print(f"{'='*80}")
                    
                    # Buscar em todas as colunas
                    for col in df.columns:
                        if df[col].dtype == 'object':  # Colunas de texto
                            matches = df[df[col].astype(str).str.contains('INGAZEIRA', case=False, na=False)]
                            if not matches.empty:
                                print(f"\nEncontrado na coluna '{col}':")
                                print(matches.to_string())
                    
                    # Mostrar todas as linhas da tabela
                    print(f"\n{'='*80}")
                    print("TODAS AS LINHAS DA TABELA:")
                    print(f"{'='*80}")
                    print(df.to_string())
                    
                    # Salvar CSV
                    csv_filename = "teste_afogados_ingazeira.csv"
                    df.to_csv(csv_filename, index=False, encoding='utf-8-sig', sep=';')
                    print(f"\nCSV salvo em: {csv_filename}")
                    
                except Exception as e:
                    print(f"Erro ao processar tabela {i}: {e}")
                    import traceback
                    traceback.print_exc()
        else:
            print("\nNenhuma tabela encontrada no HTML")
            print("\nPrimeiros 2000 caracteres da resposta:")
            print("-" * 80)
            print(response.text[:2000])
            
            # Procurar por mensagens de erro ou "sem dados"
            if "sem dados" in response.text.lower() or "sem resultado" in response.text.lower():
                print("\nAVISO: Possivel mensagem de 'sem dados' encontrada no HTML")
            
    else:
        print(f"\nErro na requisicao. Status: {response.status_code}")
        print(f"Resposta: {response.text[:1000]}")
        
except Exception as e:
    print(f"\nErro na requisicao: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("TESTE CONCLUIDO")
print("=" * 80)

