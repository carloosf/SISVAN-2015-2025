"""
Script para processar dados do SISVAN via API
Coleta dados de todas as raças e idades e salva em CSV
"""
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
import os
import requests
import time
from typing import Dict, List, Optional, Tuple


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
RACAS = {
    "01": "BRANCA",
    "02": "PRETA",
    "03": "AMARELA",
    "04": "PARDA",
    "05": "INDIGENA",
    "99": "SEM_INFORMACAO"
}

# Códigos de Sexo
SEXOS = {
    "M": "MASCULINO",
    "F": "FEMININO"
}

# Fases de Idade (nu_idade_inicio, nu_idade_fim, descricao)
# Valores em anos (podem ser decimais)
FASES_IDADE = {
    1: (0, 0.5, "MENOR_DE_6_MESES"),           # 0 a 0.5 anos (menor de 6 meses)
    2: (0.5, 2, "ENTRE_6_MESES_A_2_ANOS"),     # 0.5 a 2 anos (6 meses a 2 anos)
    3: (2, 5, "ENTRE_2_ANOS_A_5_ANOS"),        # 2 a 5 anos
    4: (5, 7, "ENTRE_5_ANOS_A_7_ANOS"),        # 5 a 7 anos
    5: (7, 10, "ENTRE_7_ANOS_A_10_ANOS")       # 7 a 10 anos
}

# Parâmetros base do payload
PAYLOAD_BASE = {
    "tpRelatorio": "2",
    "coVisualizacao": "1",
    "nuAno": "2025",
    "nuMes[]": "99",  # 99 = TODOS os meses
    "tpFiltro": "M",
    "coRegiao": "",
    "coUfIbge": "26",  # Pernambuco
    "coMunicipioIbge": "99",  # 99 = Todos os municípios
    "noRegional": "",
    "st_cobertura": "99",
    "nu_ciclo_vida": "1",
    "nu_indice_cri": "1",
    "nu_indice_ado": "1",
    "nu_idade_ges": "99",
    # ds_sexo2 será definido dinamicamente
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


def processar_html_para_dataframe(html_content: str) -> Optional[pd.DataFrame]:
    """Processa HTML e retorna DataFrame limpo apenas com municípios"""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')
        
        if not tables:
            print("  ERRO: Nenhuma tabela encontrada no HTML")
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
            print("  ERRO: Nenhum dado encontrado nas tabelas")
            return None
        
        # Remover linhas completamente vazias
        df_final = df_final.dropna(how='all')
        
        # Limpar colunas
        df_final = limpar_colunas(df_final)
        
        # Verificar se temos a coluna Municipio
        if 'Municipio' not in df_final.columns:
            print("  ERRO: Coluna 'Municipio' não encontrada após limpeza")
            return None
        
        # Filtrar apenas municípios (remover totais)
        df_final = df_final[
            ~df_final['Municipio'].astype(str).str.contains('TOTAL', case=False, na=False)
        ]
        
        # Remover linhas onde o código IBGE não é numérico válido
        if 'Codigo_IBGE' in df_final.columns:
            df_final = df_final[
                df_final['Codigo_IBGE'].astype(str).str.match(r'^\d{6}$', na=False)
            ]
        
        # Resetar índice
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
    
    # Adicionar raça
    payload["ds_raca_cor2"] = raca_codigo
    
    # Adicionar sexo
    payload["ds_sexo2"] = sexo_codigo
    
    # Adicionar idade (nu_idade_inicio e nu_idade_fim)
    # Valores podem ser decimais (ex: 0.5)
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

def coletar_dados_todas_combinacoes() -> pd.DataFrame:
    """Coleta dados de todas as combinações de raça, idade e sexo"""
    print("\n" + "=" * 80)
    print("INICIANDO COLETA DE DADOS")
    print("=" * 80)
    
    # Criar sessão HTTP
    session = requests.Session()
    print("\n1. Obtendo sessão do servidor...")
    try:
        session.get(URL_INDEX, headers=HEADERS, timeout=15)
        print("   OK - Sessão obtida")
    except Exception as e:
        print(f"   ERRO ao obter sessão: {e}")
        return pd.DataFrame()
    
    # Lista para armazenar todos os DataFrames
    todos_dataframes = []
    total_combinacoes = len(RACAS) * len(FASES_IDADE) * len(SEXOS)
    combinacao_atual = 0
    
    print(f"\n2. Coletando dados de {total_combinacoes} combinações...")
    print(f"   (6 raças × 5 fases × 2 sexos = {total_combinacoes} combinações)")
    print("-" * 80)
    
    # Iterar sobre todas as combinações (raça × fase × sexo)
    for raca_codigo in RACAS.keys():
        for fase_idade in FASES_IDADE.keys():
            for sexo_codigo in SEXOS.keys():
                combinacao_atual += 1
                
                print(f"\n[{combinacao_atual}/{total_combinacoes}] Processando combinação...")
                
                # Fazer requisição
                html_content = fazer_requisicao(session, raca_codigo, fase_idade, sexo_codigo)
                
                if html_content is None:
                    print("      AVISO: Não foi possível obter dados desta combinação")
                    continue
                
                # Processar HTML
                df = processar_html_para_dataframe(html_content)
                
                if df is None or df.empty:
                    print("      AVISO: Nenhum dado encontrado nesta combinação")
                    continue
                
                # Adicionar colunas de identificação
                raca_nome = RACAS[raca_codigo]
                sexo_nome = SEXOS[sexo_codigo]
                _, _, fase_nome = FASES_IDADE[fase_idade]
                
                df['Raca_Codigo'] = raca_codigo
                df['Raca_Nome'] = raca_nome
                df['Sexo_Codigo'] = sexo_codigo
                df['Sexo_Nome'] = sexo_nome
                df['Fase_Idade'] = fase_idade
                df['Fase_Nome'] = fase_nome
                
                todos_dataframes.append(df)
                print(f"      OK - {len(df)} municípios encontrados")
                
                # Pequeno delay para não sobrecarregar o servidor
                time.sleep(1)
    
    # Consolidar todos os DataFrames
    print(f"\n3. Consolidando dados...")
    if not todos_dataframes:
        print("   ERRO: Nenhum dado foi coletado!")
        return pd.DataFrame()
    
    df_final = pd.concat(todos_dataframes, ignore_index=True)
    print(f"   OK - Total de {len(df_final)} registros consolidados")
    
    return df_final


# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

def main():
    """Função principal"""
    print("=" * 80)
    print("PROCESSADOR DE DADOS SISVAN - COLETA COMPLETA")
    print("=" * 80)
    print("\nConfiguração:")
    print(f"  - Raças: {len(RACAS)} ({', '.join([f'{k}-{v}' for k, v in RACAS.items()])})")
    print(f"  - Sexos: {len(SEXOS)} ({', '.join([f'{k}-{v}' for k, v in SEXOS.items()])})")
    print(f"  - Fases de Idade: {len(FASES_IDADE)}")
    for fase, (inicio, fim, nome) in FASES_IDADE.items():
        print(f"    Fase {fase}: {nome} (idade {inicio} a {fim} anos)")
    print(f"  - Total de combinações: {len(RACAS) * len(FASES_IDADE) * len(SEXOS)} "
          f"(6 raças × 5 fases × 2 sexos)")
    
    # Coletar dados
    df = coletar_dados_todas_combinacoes()
    
    if df.empty:
        print("\n" + "=" * 80)
        print("ERRO: Nenhum dado foi coletado!")
        print("=" * 80)
        return
    
    # Mostrar informações
    print(f"\n4. Informações dos dados coletados:")
    print(f"   - Total de registros: {len(df)}")
    print(f"   - Total de municípios únicos: {df['Codigo_IBGE'].nunique()}")
    print(f"   - Colunas: {', '.join(df.columns.tolist())}")
    
    # Mostrar estatísticas por raça, sexo e fase
    print(f"\n5. Estatísticas por combinação:")
    stats = df.groupby(['Raca_Nome', 'Sexo_Nome', 'Fase_Nome']).size().reset_index(name='Registros')
    print(stats.to_string(index=False))
    
    # Salvar CSV
    csv_output = "dados_sisvan_completo_racas_idades.csv"
    print(f"\n6. Salvando CSV: {csv_output}")
    try:
        df.to_csv(csv_output, index=False, encoding='utf-8-sig')
        print(f"   OK - Arquivo salvo com sucesso!")
        print(f"   - Total de linhas: {len(df)}")
        print(f"   - Total de colunas: {len(df.columns)}")
    except Exception as e:
        print(f"   ERRO ao salvar CSV: {e}")
        return
    
    print(f"\n{'=' * 80}")
    print("PROCESSAMENTO CONCLUÍDO!")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
