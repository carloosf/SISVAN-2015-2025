"""
Script para processar dados do SISVAN via API
Coleta dados de todas as raças e idades e salva em CSV
"""
import json
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import os
import requests
import time
from typing import Dict, List, Optional, Tuple

# Configuração: dados carregados de utils.json (raças, sexos, fases de idade)
UTILS_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), "utils.json")
with open(UTILS_JSON, "r", encoding="utf-8") as f:
    _utils = json.load(f)
RACAS = _utils["RACAS"]
SEXOS = _utils["SEXOS"]
# FASES_IDADE: chaves string, valores (inicio, fim, nome)
FASES_IDADE = {k: (v[0], v[1], v[2]) for k, v in _utils["FASES_IDADE"].items()}


# ============================================================================
# CONFIGURAÇÕES E CONSTANTES
# ============================================================================

# URLs da API
URL_INDEX = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index"
URL_POST = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/estadonutricional"

# Headers para requisições
HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index"
}

# Códigos de Raça/Cor


# Parâmetros base do payload
PAYLOAD_BASE = {
    "tpRelatorio": "2",
    "coVisualizacao": "1",
    "nuAno": "2024",
    "nuMes[]": "01", 
    "tpFiltro": "M",
    "coRegiao": "",
    "coUfIbge": "26", 
    "coMunicipioIbge": "261160",  
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


# ============================================================================
# FUNÇÕES DE PROCESSAMENTO DE DADOS
# ============================================================================

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


def salvar_csv_powerbi(df: pd.DataFrame, path: str) -> None:
    """Salva CSV no formato que o Power BI (PT-BR) aceita melhor: separador ; e decimal com ,"""
    df_out = df.copy()
    perc_cols = ["MuitoBaixo_Perc", "Baixo_Perc", "Adequado_Perc", "Elevado_Perc"]
    for col in perc_cols:
        if col in df_out.columns:
            df_out[col] = df_out[col].astype(str).str.replace(".", ",", regex=False)
    df_out.to_csv(path, index=False, sep=";", encoding="utf-8-sig")


# Colunas esperadas no relatório SISVAN (formato "SISVAN - Relatórios de Produção.htm")
COLUNAS_SISVAN = [
    "Regiao", "Codigo_UF", "UF", "Codigo_IBGE", "Municipio",
    "MuitoBaixo_Qtd", "MuitoBaixo_Perc", "Baixo_Qtd", "Baixo_Perc",
    "Adequado_Qtd", "Adequado_Perc", "Elevado_Qtd", "Elevado_Perc", "Total"
]


def processar_html_para_dataframe(html_content: str) -> Optional[pd.DataFrame]:
    """Processa HTML no formato SISVAN (Relatórios de Produção): extrai linhas do tbody com 14 colunas."""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')
        if not tables:
            print("  ERRO: Nenhuma tabela encontrada no HTML")
            return None
        print(f"  (Encontradas {len(tables)} tabela(s) no HTML)")
        linhas = []
        for table in tables:
            tbody = table.find('tbody')
            if not tbody:
                continue
            for tr in tbody.find_all('tr'):
                tds = tr.find_all('td')
                if len(tds) != 14:
                    continue
                row = []
                for i, td in enumerate(tds):
                    val = td.get_text(strip=True)
                    if i in (6, 8, 10, 12):
                        val = val.replace("%", "").strip()
                    row.append(val)
                linhas.append(row)
        if not linhas:
            print("  ERRO: Nenhum dado encontrado nas tabelas (nenhuma linha com 14 colunas no tbody)")
            return None
        df_final = pd.DataFrame(linhas, columns=COLUNAS_SISVAN)
        df_final = df_final[
            ~df_final['Municipio'].astype(str).str.contains('TOTAL', case=False, na=False)
        ]
        if 'Codigo_IBGE' in df_final.columns:
            df_final = df_final[
                df_final['Codigo_IBGE'].astype(str).str.match(r'^\d{6}$', na=False)
            ]
        df_final = df_final.reset_index(drop=True)
        return df_final
    except Exception as e:
        print(f"  ERRO ao processar HTML: {e}")
        import traceback
        traceback.print_exc()
        return None


# ============================================================================
# FUNÇÕES DE REQUISIÇÃO HTTP
# ============================================================================

def criar_payload(raca_codigo: str, fase_idade: int, sexo_codigo: str) -> Dict[str, str]:
    """Cria payload para requisição com raça, fase de idade e sexo específicos"""
    payload = PAYLOAD_BASE.copy()
    
    payload["ds_raca_cor2"] = raca_codigo
    
    payload["ds_sexo2"] = sexo_codigo
    
    idade_inicio, idade_fim, _ = FASES_IDADE[fase_idade]
    payload["nu_idade_inicio"] = str(idade_inicio)
    payload["nu_idade_fim"] = str(idade_fim)
    
    return payload


def fazer_requisicao(session: requests.Session, raca_codigo: str, fase_idade: int, sexo_codigo: str,
                     tentativa: int = 1, max_tentativas: int = 3) -> Optional[str]:
    """Faz requisição POST para API e retorna HTML"""
    raca_nome = RACAS.get(raca_codigo, "DESCONHECIDA")
    sexo_nome = SEXOS.get(sexo_codigo, "DESCONHECIDO")
    _, _, fase_nome = FASES_IDADE[fase_idade]
    
    print(f"    [{tentativa}/{max_tentativas}] Raça: {raca_codigo}-{raca_nome} | "
          f"Sexo: {sexo_codigo}-{sexo_nome} | Fase: {fase_idade}-{fase_nome}")
    
    payload = criar_payload(raca_codigo, fase_idade, sexo_codigo)
    
    try:
        response = session.post(URL_POST, data=payload, headers=HEADERS, timeout=30)
        
        if response.status_code == 200:
            return response.text
        else:
            print(f"      ERRO: Status {response.status_code}")
            if tentativa < max_tentativas:
                time.sleep(2)  # Aguardar antes de tentar novamente
                return fazer_requisicao(session, raca_codigo, fase_idade, sexo_codigo, tentativa + 1, max_tentativas)
            return None
            
    except Exception as e:
        print(f"      ERRO na requisição: {e}")
        if tentativa < max_tentativas:
            time.sleep(2)
            return fazer_requisicao(session, raca_codigo, fase_idade, sexo_codigo, tentativa + 1, max_tentativas)
        return None


# ============================================================================
# FUNÇÕES DE COLETA E CONSOLIDAÇÃO
# ============================================================================

def rodar_uma_vez() -> pd.DataFrame:
    """Executa 1 requisição com o payload atual (1 contexto, 1 município) para conferir se os dados batem."""
    print("\n" + "=" * 80)
    print("TESTE: 1 CONTEXTO, 1 MUNICÍPIO (payload atual)")
    print("=" * 80)
    print(f"\nPayload: coMunicipioIbge={PAYLOAD_BASE.get('coMunicipioIbge')} | "
          f"ds_raca_cor2={PAYLOAD_BASE.get('ds_raca_cor2')} | ds_sexo2={PAYLOAD_BASE.get('ds_sexo2')} | "
          f"nu_idade_inicio={PAYLOAD_BASE.get('nu_idade_inicio')} nu_idade_fim={PAYLOAD_BASE.get('nu_idade_fim')}")
    session = requests.Session()
    print("\n1. Obtendo sessão do servidor...")
    try:
        session.get(URL_INDEX, headers=HEADERS, timeout=15)
        print("   OK - Sessão obtida")
    except Exception as e:
        print(f"   ERRO ao obter sessão: {e}")
        return pd.DataFrame()
    print("\n2. Fazendo 1 requisição...")
    try:
        response = session.post(URL_POST, data=PAYLOAD_BASE, headers=HEADERS, timeout=30)
        if response.status_code != 200:
            print(f"   ERRO: Status {response.status_code}")
            return pd.DataFrame()
        print("   OK - Resposta recebida")
    except Exception as e:
        print(f"   ERRO na requisição: {e}")
        return pd.DataFrame()
    # Debug: salvar HTML para inspeção quando o parser não encontrar dados
    _debug_html = os.path.join(os.path.dirname(os.path.abspath(__file__)), "debug_response.html")
    with open(_debug_html, "w", encoding="utf-8") as f:
        f.write(response.text)
    print(f"   (HTML salvo em {os.path.basename(_debug_html)} para inspeção)")
    df = processar_html_para_dataframe(response.text)
    if df is None:
        print("   ERRO ao processar HTML")
        return pd.DataFrame()
    if df.empty:
        print("   AVISO: Nenhum dado na tabela")
    else:
        print(f"   OK - {len(df)} linha(s) na tabela")
    return df


# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

def main():
    """Teste: 1 contexto, 1 município — rodar 1x para conferir se os dados batem."""
    print("=" * 80)
    print("SISVAN - TESTE 1 CONTEXTO / 1 MUNICÍPIO")
    print("=" * 80)
    df = rodar_uma_vez()
    if df.empty:
        print("\n" + "=" * 80)
        print("Nenhum dado retornado. Verifique o payload e a resposta da API.")
        print("=" * 80)
        return
    print("\n3. Dados retornados (para conferência):")
    print(df.to_string())
    csv_output = "dados_sisvan_teste_1municipio.csv"
    print(f"\n4. Salvando CSV: {csv_output}")
    try:
        salvar_csv_powerbi(df, csv_output)
        print(f"   OK - Salvo (formato Power BI: separador ;, decimal ,).")
    except Exception as e:
        print(f"   ERRO ao salvar CSV: {e}")
    print("\n" + "=" * 80)
    print("TESTE CONCLUÍDO")
    print("=" * 80)


if __name__ == "__main__":
    main()
