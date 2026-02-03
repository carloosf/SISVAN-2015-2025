import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from io import StringIO
# --- CONFIGURAÇÕES DE ACESSO ---
url_index = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index"
url_post = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/estadonutricional"

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index"
}

# --- DICIONÁRIOS DE TRADUÇÃO (DOMÍNIO) ---
mapa_raca = {"1": "Branca", "2": "Preta", "3": "Amarela", "4": "Parda", "5": "Indígena", "6": "Sem informação"}

mapa_escolaridade = {
    "1": "Não sabe ler/escrever", "2": "Alfabetizado", "3": "Fundamental Incompleto",
    "4": "Fundamental Completo", "5": "Médio Incompleto", "6": "Médio Completo",
    "7": "Superior Incompleto", "8": "Superior Completo", "9": "Especialização/Residência",
    "10": "Mestrado", "11": "Doutorado", "12": "Pós-Doutorado",
    "13": "Médio Completo Normal Magistério", "14": "Médio Completo Normal Magistério Indígena",
    "99": "Sem informação"
}

mapa_comunidade = {"1": "Quilombola", "2": "Cigano", "3": "Ribeirinho", "0": "Não se aplica/Outros"}

config_ciclos = {
    "1": { "nome": "Crianca", "idades": [{"inicio": "0", "fim": "0.5", "label": "0-6m"}, {"inicio": "0.5", "fim": "2", "label": "6m-2a"}, {"inicio": "2", "fim": "5", "label": "2a-5a"}, {"inicio": "5", "fim": "7", "label": "5a-7a"}, {"inicio": "7", "fim": "10", "label": "7a-10a"}]},
    "2": {"nome": "Adolescente", "idades": [{"inicio": "10", "fim": "20", "label": "Adolescente"}]},
    "3": {"nome": "Adulto", "idades": [{"inicio": "20", "fim": "60", "label": "Adulto"}]},
    "4": {"nome": "Idoso", "idades": [{"inicio": "60", "fim": "120", "label": "Idoso"}]},
    "5": {"nome": "Gestante", "idades": [{"inicio": "10", "fim": "20", "label": "Gestante Adolescente"}, {"inicio": "20", "fim": "60", "label": "Gestante Adulta"}]}
}

def extrair(session, ano, sexo, raca_cod, ciclo_id, idade_conf, escolaridade_id, comunidade_id):
    payload = {
        "tpRelatorio": "2", "coVisualizacao": "1", "nuAno": str(ano), "nuMes[]": "99",
        "tpFiltro": "M", "coUfIbge": "26", "coMunicipioIbge": "99",
        "nu_ciclo_vida": ciclo_id, "nu_idade_inicio": idade_conf["inicio"],
        "nu_idade_fim": idade_conf["fim"], "nu_indice_cri": "1", "nu_indice_ado": "1", 
        "nu_idade_ges": "99", "ds_sexo2": sexo, "ds_raca_cor2": str(raca_cod),
        "co_sistema_origem": "0", "CO_POVO_COMUNIDADE": str(comunidade_id), 
        "CO_ESCOLARIDADE": str(escolaridade_id), "verTela": ""
    }
    try:
        res = session.post(url_post, data=payload, headers=headers, timeout=45)
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, 'html.parser')
            tabela = soup.find('table')
            if not tabela: return None
            
            df = pd.read_html(StringIO(str(tabela)), header=2)[0]
            # Limpeza: remove totais e linhas vazias
            df = df[~df.iloc[:, 0].str.contains("TOTAL|Total", na=False)].dropna(subset=[df.columns[0]])
            
            # Padronização de Colunas (Trata a ausência física de colunas específicas em alguns ciclos)
            colunas_fixas = ['UF', 'IBGE', 'Municipio', 'CNES', 'EAS', 
                             'MuitoBaixo_Qtd', 'MuitoBaixo_Perc', 'Baixo_Qtd', 'Baixo_Perc', 
                             'Adequado_Qtd', 'Adequado_Perc', 'Elevado_Qtd', 'Elevado_Perc', 'Total']
            
            if len(df.columns) >= 14:
                df = df.iloc[:, :14]
                df.columns = colunas_fixas

            # Injeção de Metadados (Dimensões para o Power BI)
            df['Ano'] = ano
            df['Ciclo_Vida'] = config_ciclos[ciclo_id]["nome"]
            df['Faixa_Etaria'] = idade_conf["label"]
            df['Sexo'] = 'Masculino' if sexo == 'M' else 'Feminino'
            df['Raca_Cor'] = mapa_raca.get(str(raca_cod))
            df['Escolaridade'] = mapa_escolaridade.get(str(escolaridade_id))
            df['Comunidade_Tradicional'] = mapa_comunidade.get(str(comunidade_id))
            return df
    except:
        return None

# --- EXECUÇÃO DO ROBÔ ---
session = requests.Session()
session.get(url_index)
inicio_geral = time.time()
total_geral_estimado = 2496 # Total aproximado após travas lógicas

print(">>> Iniciando extração do SISVAN PE...")

for ano in [2025]:
    for ciclo_id, info in config_ciclos.items():
        # Lógica de Separação por Pacotes
        output_file = f"base_sisvan_{info['nome']}_{ano}.csv"
        
        if os.path.exists(output_file):
            print(f"\n[PULO] Arquivo {output_file} já existe. Ignorando este ciclo.")
            continue

        print(f"\n--- Processando Pacote: {info['nome']} ---")
        iteracao_pacote = 0
        ultimo_print_monitor = time.time()

        for idade_conf in info["idades"]:
            for esc_id in mapa_escolaridade.keys():
                
                # --- TRAVAS LÓGICAS (FILTROS DE PERTINÊNCIA) ---
                if ciclo_id == "1" and esc_id != "99": continue
                if (ciclo_id == "2" or (ciclo_id == "5" and "Adolescente" in idade_conf["label"])):
                    if esc_id not in ["99", "1", "2", "3", "4", "5", "6"]: continue

                for com_id in mapa_comunidade.keys():
                    sexos = ["F"] if ciclo_id == "5" else ["M", "F"]
                    for sexo in sexos:
                        for raca_id in mapa_raca.keys():
                            iteracao_pacote += 1
                            agora = time.time()
                            
                            # Log de Monitoramento a cada 10 segundos
                            if agora - ultimo_print_monitor >= 10:
                                decorrido = time.strftime("%H:%M:%S", time.gmtime(agora - inicio_geral))
                                print(f"\n[MONITOR] Tempo: {decorrido} | Ciclo: {info['nome']} | Item: {iteracao_pacote}")
                                ultimo_print_monitor = agora

                            print(f"Progresso {info['nome']}: {idade_conf['label']} | {mapa_escolaridade[esc_id]} | {sexo}", end='\r')
                            
                            df_resultado = extrair(session, ano, sexo, raca_id, ciclo_id, idade_conf, esc_id, com_id)
                            
                            if df_resultado is not None and not df_resultado.empty:
                                tem_header = not os.path.exists(output_file)
                                df_resultado.to_csv(output_file, mode='a', index=False, header=tem_header, encoding='utf-8-sig')
                            
                            time.sleep(1.1)

print(f"\n\n>>> SUCESSO! Tempo Total: {time.strftime('%H:%M:%S', time.gmtime(time.time() - inicio_geral))}")