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


# Anos a coletar: começa em 2025 (onde há dados) e desce até 2015
ANO_MAIS_RECENTE = 2025
ANO_MAIS_ANTIGO = 2023

# Parâmetros base do payload (nuAno é definido por ano na requisição)
PAYLOAD_BASE = {
    "tpRelatorio": "2",
    "coVisualizacao": "1",
    "nuAno": "",
    "nuMes[]": "99", 
    "tpFiltro": "M",
    "coRegiao": "",
    "coUfIbge": "26", 
    "coMunicipioIbge": "99",  
    "noRegional": "",
    "st_cobertura": "99",
    "nu_ciclo_vida": "1",
    "nu_indice_cri": "1",
    "nu_indice_ado": "1",
    "nu_idade_ges": "99",
    "ds_sexo2": "",
    "ds_raca_cor2": "",
    "nu_idade_inicio": "",
    "nu_idade_fim": "",
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

def criar_payload(raca_codigo: str, fase_idade: str, sexo_codigo: str, ano: int) -> Dict[str, str]:
    """Cria payload para requisição com raça, fase de idade, sexo e ano"""
    payload = PAYLOAD_BASE.copy()
    payload["nuAno"] = str(ano)
    payload["ds_raca_cor2"] = raca_codigo
    payload["ds_sexo2"] = sexo_codigo
    idade_inicio, idade_fim, _ = FASES_IDADE[fase_idade]
    payload["nu_idade_inicio"] = str(idade_inicio)
    payload["nu_idade_fim"] = str(idade_fim)
    return payload


def fazer_requisicao(session: requests.Session, raca_codigo: str, fase_idade: str, sexo_codigo: str,
                     ano: int, tentativa: int = 1, max_tentativas: int = 3) -> Optional[str]:
    """Faz requisição POST para API e retorna HTML"""
    raca_nome = RACAS.get(raca_codigo, "DESCONHECIDA")
    sexo_nome = SEXOS.get(sexo_codigo, "DESCONHECIDO")
    _, _, fase_nome = FASES_IDADE[fase_idade]
    print(f"    [{tentativa}/{max_tentativas}] Ano {ano} | Raça: {raca_codigo}-{raca_nome} | "
          f"Sexo: {sexo_codigo}-{sexo_nome} | Fase: {fase_idade}-{fase_nome}")
    payload = criar_payload(raca_codigo, fase_idade, sexo_codigo, ano)
    try:
        response = session.post(URL_POST, data=payload, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            return response.text
        print(f"      ERRO: Status {response.status_code}")
        if tentativa < max_tentativas:
            time.sleep(2)
            return fazer_requisicao(session, raca_codigo, fase_idade, sexo_codigo, ano, tentativa + 1, max_tentativas)
        return None
    except Exception as e:
        print(f"      ERRO na requisição: {e}")
        if tentativa < max_tentativas:
            time.sleep(2)
            return fazer_requisicao(session, raca_codigo, fase_idade, sexo_codigo, ano, tentativa + 1, max_tentativas)
        return None


# ============================================================================
# FUNÇÕES DE COLETA E CONSOLIDAÇÃO
# ============================================================================

def coletar_dados_para_ano(ano: int) -> pd.DataFrame:
    """Coleta dados de todas as combinações (raça, fase, sexo) para um único ano. Adiciona coluna Ano."""
    print("\n" + "=" * 80)
    print(f"COLETANDO DADOS DO ANO {ano}")
    print("=" * 80)
    # Mostrar payload usado neste ano (exemplo com primeira combinação)
    payload_ano = criar_payload(
        next(iter(RACAS.keys())),
        next(iter(FASES_IDADE.keys())),
        next(iter(SEXOS.keys())),
        ano,
    )
    print(f"\nPayload para ano {ano}:")
    for k, v in sorted(payload_ano.items()):
        print(f"  {k}: {v}")
    session = requests.Session()
    print("\n1. Obtendo sessão do servidor...")
    try:
        session.get(URL_INDEX, headers=HEADERS, timeout=15)
        print("   OK - Sessão obtida")
    except Exception as e:
        print(f"   ERRO ao obter sessão: {e}")
        return pd.DataFrame()
    todos_dataframes = []
    total_combinacoes = len(RACAS) * len(FASES_IDADE) * len(SEXOS)
    combinacao_atual = 0
    print(f"\n2. Coletando dados de {total_combinacoes} combinações para {ano}...")
    print("-" * 80)
    for raca_codigo in RACAS.keys():
        for fase_idade in FASES_IDADE.keys():
            for sexo_codigo in SEXOS.keys():
                combinacao_atual += 1
                print(f"\n[{combinacao_atual}/{total_combinacoes}] Processando combinação...")
                html_content = fazer_requisicao(session, raca_codigo, fase_idade, sexo_codigo, ano)
                if html_content is None:
                    print("      AVISO: Não foi possível obter dados desta combinação")
                    continue
                df = processar_html_para_dataframe(html_content)
                if df is None or df.empty:
                    print("      AVISO: Nenhum dado encontrado nesta combinação")
                    continue
                raca_nome = RACAS[raca_codigo]
                sexo_nome = SEXOS[sexo_codigo]
                _, _, fase_nome = FASES_IDADE[fase_idade]
                df["Ano"] = ano
                df["Raca_Codigo"] = raca_codigo
                df["Raca_Nome"] = raca_nome
                df["Sexo_Codigo"] = sexo_codigo
                df["Sexo_Nome"] = sexo_nome
                df["Fase_Idade"] = fase_idade
                df["Fase_Nome"] = fase_nome
                todos_dataframes.append(df)
                print(f"      OK - {len(df)} municípios encontrados")
                time.sleep(1)
    print(f"\n3. Consolidando dados do ano {ano}...")
    if not todos_dataframes:
        print("   ERRO: Nenhum dado foi coletado!")
        return pd.DataFrame()
    df_final = pd.concat(todos_dataframes, ignore_index=True)
    print(f"   OK - Total de {len(df_final)} registros para {ano}")
    return df_final


# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

def main():
    """Coleta por ano e salva um CSV por ano (com coluna Ano)."""
    print("=" * 80)
    print("PROCESSADOR DE DADOS SISVAN - COLETA POR ANO")
    print("=" * 80)
    print("\nConfiguração:")
    print(f"  - Anos: {ANO_MAIS_RECENTE} → {ANO_MAIS_ANTIGO} (começa em 2025 e desce)")
    print(f"  - Raças: {len(RACAS)} | Sexos: {len(SEXOS)} | Fases de Idade: {len(FASES_IDADE)}")
    print(f"  - Total de combinações por ano: {len(RACAS) * len(FASES_IDADE) * len(SEXOS)}")
    for ano in range(ANO_MAIS_RECENTE, ANO_MAIS_ANTIGO - 1, -1):
        df = coletar_dados_para_ano(ano)
        if df.empty:
            print(f"\n   AVISO: Nenhum dado para {ano}, pulando.")
            continue
        csv_output = f"dados_sisvan_racas_idades_{ano}.csv"
        print(f"\n4. Salvando {csv_output} ({len(df)} registros, coluna Ano={ano})")
        try:
            salvar_csv_powerbi(df, csv_output)
            print(f"   OK - {csv_output} salvo.")
        except Exception as e:
            print(f"   ERRO ao salvar: {e}")
    print(f"\n{'=' * 80}")
    print("PROCESSAMENTO CONCLUÍDO!")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
