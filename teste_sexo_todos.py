"""
Script de teste para descobrir o valor correto de ds_sexo2 para "TODOS"
"""
import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO

URL_INDEX = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index"
URL_POST = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/estadonutricional"

HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index"
}

# Valores para testar
valores_teste = [
    ("", "String vazia"),
    ("99", "Valor 99"),
    ("TODOS", "String TODOS"),
    # Omitir campo (None)
]

session = requests.Session()
session.get(URL_INDEX, headers=HEADERS, timeout=15)

print("=" * 80)
print("TESTE: Valores para ds_sexo2 = TODOS")
print("=" * 80)
print("\nTestando com BODOCO (260200), Fase 1 (0-0.5 anos), Raça 99\n")

for valor, descricao in valores_teste:
    print(f"\n{'='*80}")
    print(f"Teste: {descricao} (valor: '{valor}')")
    print(f"{'='*80}")
    
    payload = {
        "tpRelatorio": "2",
        "coVisualizacao": "1",
        "nuAno": "2025",
        "nuMes[]": "99",
        "tpFiltro": "M",
        "coRegiao": "",
        "coUfIbge": "26",
        "coMunicipioIbge": "260200",  # BODOCO específico
        "noRegional": "",
        "st_cobertura": "99",
        "nu_ciclo_vida": "1",
        "nu_idade_inicio": "0",
        "nu_idade_fim": "0.5",
        "nu_indice_cri": "1",
        "nu_indice_ado": "1",
        "nu_idade_ges": "99",
        "ds_raca_cor2": "99",  # SEM_INFORMACAO
        "co_sistema_origem": "0",
        "CO_POVO_COMUNIDADE": "TODOS",
        "CO_ESCOLARIDADE": "TODOS",
        "verTela": ""
    }
    
    # Adicionar ou omitir ds_sexo2
    if valor is not None:
        payload["ds_sexo2"] = valor
    
    try:
        response = session.post(URL_POST, data=payload, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Verificar o que aparece no filtro
            filtros = soup.find('div', class_='box-body')
            if filtros:
                texto_filtros = filtros.get_text()
                if "Sexo:" in texto_filtros:
                    linha_sexo = [l for l in texto_filtros.split('\n') if 'Sexo:' in l]
                    if linha_sexo:
                        print(f"  Filtro mostrado: {linha_sexo[0].strip()}")
            
            # Tentar extrair dados de BODOCO
            tables = soup.find_all('table')
            if tables:
                try:
                    df = pd.read_html(StringIO(str(tables[0])))[0]
                    # Procurar BODOCO
                    df_str = df.astype(str)
                    bodoco = df_str[df_str.apply(lambda x: x.str.contains('260200|BODOCO', case=False, na=False).any(), axis=1)]
                    
                    if not bodoco.empty:
                        print(f"  Dados encontrados para BODOCO:")
                        print(f"  {bodoco.iloc[0].to_dict()}")
                    else:
                        print(f"  BODOCO não encontrado na tabela")
                except:
                    print(f"  Erro ao processar tabela")
        else:
            print(f"  ERRO: Status {response.status_code}")
            
    except Exception as e:
        print(f"  ERRO: {e}")
    
    import time
    time.sleep(2)  # Delay entre requisições

print(f"\n{'='*80}")
print("TESTE CONCLUÍDO")
print(f"{'='*80}")

