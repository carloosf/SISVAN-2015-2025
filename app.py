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

# --- DICIONÁRIOS DE TRADUÇÃO (DOMÍNIO) ---
# Código da Raça/Cor conforme dicionário SISVAN
mapa_raca = {
    "01": "Branca", "02": "Preta", "03": "Amarela", "04": "Parda", 
    "05": "Indígena", "99": "Sem informação"
}

# Código da Escolaridade conforme dicionário SISVAN
mapa_escolaridade = {
    "1": "Creche",
    "2": "Pré-escola (exceto CA)",
    "3": "Classe Alfabetizada - CA",
    "4": "Ensino Fundamental 1ª a 4ª séries",
    "5": "Ensino Fundamental 5ª a 8ª séries",
    "6": "Ensino Fundamental Completo",
    "7": "Ensino Fundamental Especial",
    "8": "Ensino Fundamental EJA - séries iniciais (Supletivo 1ª a 4ª)",
    "9": "Ensino Fundamental EJA - séries iniciais (Supletivo 5ª a 8ª)",
    "10": "Ensino Médio, Médio 2º Ciclo (Científico, Técnico e etc)",
    "11": "Ensino Médio Especial",
    "12": "Ensino Médio EJA (Supletivo)",
    "13": "Superior, Aperfeiçoamento, Especialização, Mestrado, Doutorado",
    "14": "Alfabetização para Adultos (Mobral, etc)",
    "15": "Nenhum",
    "99": "Sem informação"
}

# Código do Povo ou Comunidade Tradicional conforme dicionário SISVAN
mapa_comunidade = {
    "1": "Povos Quilombolas",
    "2": "Agroextrativistas",
    "3": "Caatingueiros",
    "4": "Caiçaras",
    "5": "Comunidades de Fundo e Fecho de Pasto",
    "6": "Comunidades do Cerrado",
    "7": "Extrativistas",
    "8": "Faxinalenses",
    "9": "Geraizeiros",
    "10": "Marisqueiros",
    "11": "Pantaneiros",
    "12": "Pescadores Artesanais",
    "13": "Pomeranos",
    "14": "Povos Ciganos",
    "15": "Povos de Terreiro",
    "16": "Quebradeiras de Coco-de-Babaçu",
    "17": "Retireiros",
    "18": "Ribeirinhos",
    "19": "Seringueiros",
    "20": "Vazanteiros",
    "21": "Outros",
    "0": "Não se aplica/Outros"
}

config_ciclos = {
    "1": { "nome": "Crianca", "idades": [{"inicio": "0", "fim": "0.5", "label": "0-6m"}, {"inicio": "0.5", "fim": "2", "label": "6m-2a"}, {"inicio": "2", "fim": "5", "label": "2a-5a"}, {"inicio": "5", "fim": "7", "label": "5a-7a"}, {"inicio": "7", "fim": "10", "label": "7a-10a"}]},
    "2": {"nome": "Adolescente", "idades": [{"inicio": "10", "fim": "20", "label": "Adolescente"}]},
    "3": {"nome": "Adulto", "idades": [{"inicio": "20", "fim": "60", "label": "Adulto"}]},
    "4": {"nome": "Idoso", "idades": [{"inicio": "60", "fim": "120", "label": "Idoso"}]},
    "5": {"nome": "Gestante", "idades": [{"inicio": "10", "fim": "20", "label": "Gestante Adolescente"}, {"inicio": "20", "fim": "60", "label": "Gestante Adulta"}]}
}

# --- FUNÇÕES AUXILIARES PARA NORMALIZAÇÃO DE CÓDIGOS ---
def normalizar_codigo_raca(codigo):
    """Normaliza código de raça para formato da API (sem zero à esquerda)"""
    # Mapeia códigos do dicionário para formato da API
    mapeamento = {"01": "1", "02": "2", "03": "3", "04": "4", "05": "5", "99": "99"}
    return mapeamento.get(codigo, codigo)

def obter_nome_raca(codigo):
    """Obtém o nome da raça baseado no código (aceita ambos formatos)"""
    # Converte código da API para formato do dicionário
    codigo_str = str(codigo)
    if codigo_str in codigos_raca_dict:
        codigo_dict = codigos_raca_dict[codigo_str]
        return mapa_raca.get(codigo_dict, "Sem informação")
    # Se já está no formato do dicionário
    return mapa_raca.get(codigo_str, "Sem informação")

# Listas de códigos para iteração (formato compatível com API)
# A API espera códigos sem zero à esquerda, mas mapeamos para nomes do dicionário
codigos_raca_api = ["1", "2", "3", "4", "5", "99"]  # Formato API
codigos_raca_dict = {"1": "01", "2": "02", "3": "03", "4": "04", "5": "05", "99": "99"}  # Mapeamento para dicionário

def extrair(session, ano, sexo, raca_cod, ciclo_id, idade_conf, escolaridade_id, comunidade_id, debug=False):
    # Define o índice correto baseado no ciclo de vida
    # Para adolescentes: nu_indice_ado: "2" = IMC (o que queremos)
    # Para adolescentes: nu_indice_ado: "1" = Altura (não é o que queremos)
    indice_ado = "2" if ciclo_id == "2" else "1"  # IMC para adolescentes
    
    payload = {
        "tpRelatorio": "2", "coVisualizacao": "1", "nuAno": str(ano), "nuMes[]": "99",
        "tpFiltro": "M", "coUfIbge": "26", "coMunicipioIbge": "99",
        "nu_ciclo_vida": ciclo_id, "nu_idade_inicio": idade_conf["inicio"],
        "nu_idade_fim": idade_conf["fim"], "nu_indice_cri": "1", "nu_indice_ado": indice_ado, 
        "nu_idade_ges": "99", "ds_sexo2": sexo, "ds_raca_cor2": str(raca_cod),
        "co_sistema_origem": "0", "CO_POVO_COMUNIDADE": str(comunidade_id), 
        "CO_ESCOLARIDADE": str(escolaridade_id), "verTela": ""
    }
    
    # DEBUG: Mostra payload e resposta
    if debug:
        print("\n" + "="*80)
        print("DEBUG - PAYLOAD ENVIADO:")
        print("="*80)
        for key, value in payload.items():
            print(f"  {key}: {value}")
        print("="*80)
    
    try:
        res = session.post(url_post, data=payload, headers=headers, timeout=45)
        
        if debug:
            print(f"\nSTATUS CODE: {res.status_code}")
            print(f"TAMANHO DA RESPOSTA: {len(res.text)} caracteres")
            print("\nPRIMEIROS 2000 CARACTERES DA RESPOSTA HTML:")
            print("-"*80)
            print(res.text[:2000])
            print("-"*80)
        
        if res.status_code == 200:
            soup = BeautifulSoup(res.text, 'html.parser')
            tabela = soup.find('table')
            if not tabela: 
                if debug:
                    print("\n[AVISO] Nenhuma tabela encontrada no HTML!")
                return None
            
            if debug:
                print("\nTABELA HTML ENCONTRADA:")
                print("-"*80)
                print(str(tabela)[:1500])
                print("-"*80)
            
            df = pd.read_html(StringIO(str(tabela)), header=2)[0]
            
            if debug:
                print("\nDATAFRAME EXTRAÍDO (PRIMEIRAS 5 LINHAS):")
                print("-"*80)
                print(df.head())
                print("-"*80)
                print(f"COLUNAS: {list(df.columns)}")
                print(f"FORMATO: {df.shape}")
                print("="*80)
            
            # Verifica se o DataFrame tem dados
            if df.empty:
                return None
            
            # Limpeza: remove linha de cabeçalho duplicada e totais
            # A linha de cabeçalho duplicada geralmente tem valores que correspondem aos nomes das colunas
            primeira_col_nome = df.columns[0]
            
            # Remove linha onde a primeira coluna é igual ao nome da primeira coluna
            df = df[df.iloc[:, 0].astype(str) != primeira_col_nome]
            
            # Remove linhas onde a primeira coluna contém apenas texto de cabeçalho
            # (como "Código UF", "Código IBGE", etc.)
            df = df[~df.iloc[:, 0].astype(str).str.match("^(Região|Código UF|Código IBGE|UF|Município)$", na=False)]
            
            # Remove linhas de TOTAL
            df = df[~df.iloc[:, 0].astype(str).str.contains("TOTAL|Total", na=False, case=False)]
            
            # Remove linhas completamente vazias ou onde a primeira coluna é NaN
            df = df.dropna(subset=[df.columns[0]])
            
            # Verifica se ainda tem dados após limpeza
            if df.empty:
                return None
            
            # Detecta o tipo de tabela baseado no número de colunas
            num_cols = len(df.columns)
            
            if num_cols == 18:
                # Tabela IMC (para adolescentes com nu_indice_ado: 2)
                # Formato: Região, Código UF, UF, Código IBGE, Município, 
                #          Magreza acentuada (Qtd, %), Magreza (Qtd, %), Eutrofia (Qtd, %),
                #          Sobrepeso (Qtd, %), Obesidade (Qtd, %), Obesidade Grave (Qtd, %), Total
                # Mapeia para: UF, IBGE, Municipio, CNES, EAS, MuitoBaixo, Baixo, Adequado, Elevado, Total
                # CNES e EAS não vêm na tabela, então serão vazios
                if len(df.columns) >= 18:
                    # Extrai as colunas relevantes e mapeia
                    # Colunas originais: 0=Região, 1=Código UF, 2=UF, 3=Código IBGE, 4=Município,
                    #                    5=Magreza acentuada Qtd, 6=Magreza acentuada %, 
                    #                    7=Magreza Qtd, 8=Magreza %,
                    #                    9=Eutrofia Qtd, 10=Eutrofia %,
                    #                    11=Sobrepeso Qtd, 12=Sobrepeso %,
                    #                    13=Obesidade Qtd, 14=Obesidade %,
                    #                    15=Obesidade Grave Qtd, 16=Obesidade Grave %,
                    #                    17=Total
                    df_mapped = pd.DataFrame()
                    df_mapped['UF'] = df.iloc[:, 2]  # UF
                    df_mapped['IBGE'] = df.iloc[:, 3]  # Código IBGE
                    df_mapped['Municipio'] = df.iloc[:, 4]  # Município
                    df_mapped['CNES'] = ''  # Não disponível
                    df_mapped['EAS'] = ''  # Não disponível
                    df_mapped['MuitoBaixo_Qtd'] = df.iloc[:, 5]  # Magreza acentuada Qtd
                    df_mapped['MuitoBaixo_Perc'] = df.iloc[:, 6]  # Magreza acentuada %
                    df_mapped['Baixo_Qtd'] = df.iloc[:, 7]  # Magreza Qtd
                    df_mapped['Baixo_Perc'] = df.iloc[:, 8]  # Magreza %
                    df_mapped['Adequado_Qtd'] = df.iloc[:, 9]  # Eutrofia Qtd
                    df_mapped['Adequado_Perc'] = df.iloc[:, 10]  # Eutrofia %
                    # Elevado = Sobrepeso + Obesidade + Obesidade Grave
                    df_mapped['Elevado_Qtd'] = pd.to_numeric(df.iloc[:, 11], errors='coerce').fillna(0) + \
                                               pd.to_numeric(df.iloc[:, 13], errors='coerce').fillna(0) + \
                                               pd.to_numeric(df.iloc[:, 15], errors='coerce').fillna(0)
                    # Para percentual, calcula baseado no total
                    total = pd.to_numeric(df.iloc[:, 17], errors='coerce').fillna(0)
                    df_mapped['Elevado_Perc'] = (df_mapped['Elevado_Qtd'] / total * 100).replace([float('inf'), float('-inf')], '-').fillna('-')
                    df_mapped['Total'] = df.iloc[:, 17]  # Total
                    df = df_mapped
            elif num_cols >= 14:
                # Tabela padrão (Altura ou outras métricas)
                colunas_fixas = ['UF', 'IBGE', 'Municipio', 'CNES', 'EAS', 
                                 'MuitoBaixo_Qtd', 'MuitoBaixo_Perc', 'Baixo_Qtd', 'Baixo_Perc', 
                                 'Adequado_Qtd', 'Adequado_Perc', 'Elevado_Qtd', 'Elevado_Perc', 'Total']
                df = df.iloc[:, :14]
                df.columns = colunas_fixas
            else:
                # Se não tem colunas suficientes, tenta usar as que tem
                if len(df.columns) > 0:
                    colunas_fixas = ['UF', 'IBGE', 'Municipio', 'CNES', 'EAS', 
                                     'MuitoBaixo_Qtd', 'MuitoBaixo_Perc', 'Baixo_Qtd', 'Baixo_Perc', 
                                     'Adequado_Qtd', 'Adequado_Perc', 'Elevado_Qtd', 'Elevado_Perc', 'Total']
                    # Adiciona colunas faltantes com valores vazios
                    for i in range(len(df.columns), 14):
                        df[f'Col_{i}'] = ''
                    df.columns = colunas_fixas

            # Injeção de Metadados (Dimensões para o Power BI)
            df['Ano'] = ano
            df['Ciclo_Vida'] = config_ciclos[ciclo_id]["nome"]
            df['Faixa_Etaria'] = idade_conf["label"]
            df['Sexo'] = 'Masculino' if sexo == 'M' else 'Feminino'
            df['Raca_Cor'] = obter_nome_raca(str(raca_cod))
            df['Escolaridade'] = mapa_escolaridade.get(str(escolaridade_id), "Sem informação")
            df['Comunidade_Tradicional'] = mapa_comunidade.get(str(comunidade_id), "Não se aplica/Outros")
            
            return df
    except Exception as e:
        print(f"\n[ERRO na extração] {str(e)}")
        return None

# --- FUNÇÕES DE CHECKPOINT ---
def salvar_checkpoint(ano, ciclo_id, ciclo_nome, iteracao_pacote, idade_label, esc_id, com_id, sexo, raca_id):
    """Salva o progresso atual em um arquivo de checkpoint"""
    checkpoint = {
        "ano": ano,
        "ciclo_id": ciclo_id,
        "ciclo_nome": ciclo_nome,
        "iteracao_pacote": iteracao_pacote,
        "idade_label": idade_label,
        "esc_id": esc_id,
        "com_id": com_id,
        "sexo": sexo,
        "raca_id": raca_id
    }
    with open(f"checkpoint_{ano}.txt", 'w', encoding='utf-8') as f:
        json.dump(checkpoint, f, indent=2)

def carregar_checkpoint(ano):
    """Carrega o checkpoint se existir"""
    checkpoint_file = f"checkpoint_{ano}.txt"
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return None
    return None

def limpar_checkpoint(ano):
    """Remove o arquivo de checkpoint"""
    checkpoint_file = f"checkpoint_{ano}.txt"
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)

# --- EXECUÇÃO DO ROBÔ ---
session = requests.Session()
session.get(url_index)
inicio_geral = time.time()
total_geral_estimado = 2496 # Total aproximado após travas lógicas

print(">>> Iniciando extração do SISVAN PE...")

for ano in [2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025]:
    # Verifica se existe checkpoint para continuar de onde parou
    checkpoint = carregar_checkpoint(ano)
    continuar_de_checkpoint = checkpoint is not None
    
    if continuar_de_checkpoint:
        print(f"\n[CHECKPOINT ENCONTRADO] Continuando de onde parou:")
        print(f"  - Ciclo: {checkpoint['ciclo_nome']}")
        print(f"  - Requisições já processadas: {checkpoint['iteracao_pacote']}")
    
    # Converte para lista para poder calcular progresso
    ciclos_lista = list(config_ciclos.items())
    total_ciclos = len(ciclos_lista)
    
    # Flag para controlar se já passou do ponto de checkpoint
    checkpoint_processado = not continuar_de_checkpoint
    
    for idx, (ciclo_id, info) in enumerate(ciclos_lista, 1):
        # Lógica de Separação por Pacotes
        output_file = f"base_sisvan_{info['nome']}_{ano}.csv"
        
        if os.path.exists(output_file) and not continuar_de_checkpoint:
            print(f"\n[PULO] Arquivo {output_file} já existe. Ignorando este ciclo.")
            continue

        # Se tem checkpoint e ainda não chegou no ciclo do checkpoint, pula
        if continuar_de_checkpoint and not checkpoint_processado:
            if ciclo_id != checkpoint['ciclo_id']:
                print(f"\n[PULO] Pulando ciclo {info['nome']} (já processado ou antes do checkpoint)")
                continue
            else:
                checkpoint_processado = True
                print(f"\n[RETOMANDO] Continuando do ciclo {info['nome']}")

        ciclos_faltam = total_ciclos - idx
        print(f"\n--- Processando Ciclo {idx}/{total_ciclos}: {info['nome']} | Faltam {ciclos_faltam} ciclo(s) ---")
        iteracao_pacote = 0
        ultimo_print_monitor = time.time()
        ultimo_checkpoint = time.time()
        
        # Para Criança: cria arquivo com header no início (mesmo que vazio)
        if ciclo_id == "1" and not os.path.exists(output_file):
            colunas = ['UF', 'IBGE', 'Municipio', 'CNES', 'EAS', 
                      'MuitoBaixo_Qtd', 'MuitoBaixo_Perc', 'Baixo_Qtd', 'Baixo_Perc', 
                      'Adequado_Qtd', 'Adequado_Perc', 'Elevado_Qtd', 'Elevado_Perc', 'Total',
                      'Ano', 'Ciclo_Vida', 'Faixa_Etaria', 'Sexo', 'Raca_Cor', 'Escolaridade', 'Comunidade_Tradicional']
            df_vazio = pd.DataFrame(columns=colunas)
            try:
                with open(output_file, 'w', encoding='utf-8-sig', newline='') as f:
                    df_vazio.to_csv(f, index=False)
                print(f"\n[ARQUIVO CRIADO] {output_file} (vazio, aguardando dados...)")
            except Exception as e:
                print(f"\n[ERRO ao criar arquivo] {e}")

        for idade_conf in info["idades"]:
            # Para Criança: ignora escolaridade e comunidades tradicionais
            if ciclo_id == "1":
                esc_id = "99"  # Sem informação (valor padrão)
                com_id = "0"   # Não se aplica/Outros (valor padrão)
                
                sexos = ["F"] if ciclo_id == "5" else ["M", "F"]
                for sexo in sexos:
                    for raca_id in codigos_raca_api:
                        # Se tem checkpoint, pula até chegar no ponto exato
                        if continuar_de_checkpoint and not checkpoint_processado:
                            if (idade_conf["label"] == checkpoint['idade_label'] and 
                                esc_id == checkpoint['esc_id'] and 
                                com_id == checkpoint['com_id'] and 
                                sexo == checkpoint['sexo'] and 
                                raca_id == checkpoint['raca_id']):
                                checkpoint_processado = True
                                iteracao_pacote = checkpoint['iteracao_pacote']
                                print(f"\n[RETOMANDO] Última requisição processada: {iteracao_pacote}. Continuando da próxima...")
                                continue  # Pula a requisição já processada
                            else:
                                continue
                        
                        iteracao_pacote += 1
                        agora = time.time()
                        
                        # Log de Monitoramento a cada 10 segundos
                        if agora - ultimo_print_monitor >= 10:
                            decorrido = time.strftime("%H:%M:%S", time.gmtime(agora - inicio_geral))
                            print(f"\n[MONITOR] Tempo: {decorrido} | Ciclo: {info['nome']} | Requisições processadas: {iteracao_pacote}")
                            ultimo_print_monitor = agora

                        print(f"Progresso {info['nome']}: {idade_conf['label']} | {mapa_escolaridade[esc_id]} | {sexo}", end='\r')
                        
                        df_resultado = extrair(session, ano, sexo, raca_id, ciclo_id, idade_conf, esc_id, com_id)
                        
                        # Salvamento incremental durante o processamento
                        if df_resultado is not None and not df_resultado.empty:
                            tem_header = not os.path.exists(output_file)
                            try:
                                # Abre, escreve e fecha imediatamente para garantir escrita em disco
                                with open(output_file, 'a', encoding='utf-8-sig', newline='') as f:
                                    df_resultado.to_csv(f, index=False, header=tem_header)
                                    f.flush()  # Força escrita imediata em disco
                                    os.fsync(f.fileno())  # Garante sincronização com o sistema de arquivos
                                # Log de sucesso apenas na primeira escrita de cada ciclo
                                if tem_header:
                                    print(f"\n[ARQUIVO CRIADO] {output_file} | Linhas salvas: {len(df_resultado)}")
                            except Exception as e:
                                print(f"\n[ERRO ao salvar CSV] {e}")
                        else:
                            # Log de debug para entender por que não está salvando
                            if ciclo_id == "1" and iteracao_pacote <= 3:  # Apenas nas primeiras 3 tentativas
                                if df_resultado is None:
                                    print(f"\n[DEBUG CRIANCA] Requisição {iteracao_pacote}: df_resultado é None")
                                elif df_resultado.empty:
                                    print(f"\n[DEBUG CRIANCA] Requisição {iteracao_pacote}: df_resultado está vazio")
                        
                        # Salva checkpoint após cada requisição (com intervalo mínimo de 2s para performance)
                        if agora - ultimo_checkpoint >= 2:
                            salvar_checkpoint(ano, ciclo_id, info['nome'], iteracao_pacote, 
                                            idade_conf['label'], esc_id, com_id, sexo, raca_id)
                            ultimo_checkpoint = agora
                        
                        time.sleep(1.1)
            else:
                # Para outros ciclos: itera sobre escolaridade e comunidades
                for esc_id in mapa_escolaridade.keys():
                    
                    # --- TRAVAS LÓGICAS (FILTROS DE PERTINÊNCIA) ---
                    if (ciclo_id == "2" or (ciclo_id == "5" and "Adolescente" in idade_conf["label"])):
                        if esc_id not in ["99", "1", "2", "3", "4", "5", "6"]: continue

                    for com_id in mapa_comunidade.keys():
                        sexos = ["F"] if ciclo_id == "5" else ["M", "F"]
                        for sexo in sexos:
                            for raca_id in codigos_raca_api:
                                # Se tem checkpoint, pula até chegar no ponto exato
                                if continuar_de_checkpoint and not checkpoint_processado:
                                    if (idade_conf["label"] == checkpoint['idade_label'] and 
                                        esc_id == checkpoint['esc_id'] and 
                                        com_id == checkpoint['com_id'] and 
                                        sexo == checkpoint['sexo'] and 
                                        raca_id == checkpoint['raca_id']):
                                        checkpoint_processado = True
                                        iteracao_pacote = checkpoint['iteracao_pacote']
                                        print(f"\n[RETOMANDO] Última requisição processada: {iteracao_pacote}. Continuando da próxima...")
                                        continue  # Pula a requisição já processada
                                    else:
                                        continue
                                
                                iteracao_pacote += 1
                                agora = time.time()
                                
                                # Log de Monitoramento a cada 10 segundos
                                if agora - ultimo_print_monitor >= 10:
                                    decorrido = time.strftime("%H:%M:%S", time.gmtime(agora - inicio_geral))
                                    print(f"\n[MONITOR] Tempo: {decorrido} | Ciclo: {info['nome']} | Requisições processadas: {iteracao_pacote}")
                                    ultimo_print_monitor = agora

                                print(f"Progresso {info['nome']}: {idade_conf['label']} | {mapa_escolaridade[esc_id]} | {sexo}", end='\r')
                                
                                df_resultado = extrair(session, ano, sexo, raca_id, ciclo_id, idade_conf, esc_id, com_id)
                                
                                # Salvamento incremental durante o processamento
                                if df_resultado is not None and not df_resultado.empty:
                                    tem_header = not os.path.exists(output_file)
                                    try:
                                        # Abre, escreve e fecha imediatamente para garantir escrita em disco
                                        with open(output_file, 'a', encoding='utf-8-sig', newline='') as f:
                                            df_resultado.to_csv(f, index=False, header=tem_header)
                                            f.flush()  # Força escrita imediata em disco
                                            os.fsync(f.fileno())  # Garante sincronização com o sistema de arquivos
                                        # Log de sucesso apenas na primeira escrita de cada ciclo
                                        if tem_header:
                                            print(f"\n[ARQUIVO CRIADO] {output_file} | Linhas salvas: {len(df_resultado)}")
                                    except Exception as e:
                                        print(f"\n[ERRO ao salvar CSV] {e}")
                                # Removido logs de aviso para não poluir o output - apenas salva quando tem dados
                                
                                # Salva checkpoint após cada requisição (com intervalo mínimo de 2s para performance)
                                if agora - ultimo_checkpoint >= 2:
                                    salvar_checkpoint(ano, ciclo_id, info['nome'], iteracao_pacote, 
                                                    idade_conf['label'], esc_id, com_id, sexo, raca_id)
                                    ultimo_checkpoint = agora
                                
                                time.sleep(1.1)
        
        # Limpa checkpoint ao finalizar um ciclo completo
        if checkpoint and ciclo_id == checkpoint.get('ciclo_id'):
            limpar_checkpoint(ano)
            print(f"\n[CHECKPOINT LIMPO] Ciclo {info['nome']} concluído!")

# Limpa checkpoint final se tudo foi concluído
limpar_checkpoint(ano)

print(f"\n\n>>> SUCESSO! Tempo Total: {time.strftime('%H:%M:%S', time.gmtime(time.time() - inicio_geral))}")