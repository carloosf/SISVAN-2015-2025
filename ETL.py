import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

URL_INDEX = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index"
URL_POST = "https://sisaps.saude.gov.br/sisvan/relatoriopublico/estadonutricional"
HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://sisaps.saude.gov.br/sisvan/relatoriopublico/index",
}

# Payload base conforme sua regra específica para Adultos/Idosos
PAYLOAD_BASE = {
    "tpRelatorio": "2",
    "coVisualizacao": "1",
    "nuAno": "2025",
    "nuMes[]": "99",
    "tpFiltro": "M",
    "coRegiao": "",
    "coUfIbge": "26",
    "coMunicipioIbge": "99",
    "noRegional": "",
    "st_cobertura": "99",
    "nu_ciclo_vida": "3",
    "nu_idade_inicio": "",
    "nu_idade_fim": "-SELECIONE-",
    "nu_indice_cri": "1",
    "nu_indice_ado": "1",
    "nu_idade_ges": "99",
    "ds_sexo2": "F",
    "ds_raca_cor2": "02",
    "co_sistema_origem": "0",
    "CO_POVO_COMUNIDADE": "21",
    "CO_ESCOLARIDADE": "99",
    "verTela": "",
}

# Formato do relatório Adulto/Idoso (IMC): 18 colunas - igual a "SISVAN - Relatórios de Produção.htm"
COLUNAS_SISVAN = [
    "Regiao", "Codigo_UF", "UF", "Codigo_IBGE", "Municipio",
    "BaixoPeso_Qtd", "BaixoPeso_Perc",
    "Adequado_Qtd", "Adequado_Perc",
    "Sobrepeso_Qtd", "Sobrepeso_Perc",
    "ObesidadeI_Qtd", "ObesidadeI_Perc",
    "ObesidadeII_Qtd", "ObesidadeII_Perc",
    "ObesidadeIII_Qtd", "ObesidadeIII_Perc",
    "Total",
]
# Índices das colunas de percentual (para remover "%")
IDX_PERC = (6, 8, 10, 12, 14, 16)

# Colunas de percentual para formatação no CSV (decimal com vírgula)
COLUNAS_PERC = [
    "BaixoPeso_Perc", "Adequado_Perc", "Sobrepeso_Perc",
    "ObesidadeI_Perc", "ObesidadeII_Perc", "ObesidadeIII_Perc",
]


def salvar_csv(df: pd.DataFrame, path: str) -> None:
    """Salva DataFrame em CSV: separador ; e encoding UTF-8 com BOM (Power BI / Excel)."""
    df_out = df.copy()
    for col in COLUNAS_PERC:
        if col in df_out.columns:
            df_out[col] = df_out[col].astype(str).str.replace(".", ",", regex=False)
    df_out.to_csv(path, index=False, sep=";", encoding="utf-8-sig")


def processar_html_para_dataframe(html_content: str):
    """Extrai linhas da tabela HTML no formato SISVAN Adulto/IMC (tbody, 18 colunas)."""
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        tables = soup.find_all("table")
        linhas = []
        for table in tables:
            tbody = table.find("tbody")
            if not tbody:
                continue
            for tr in tbody.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) != 18:
                    continue
                row = []
                for i, td in enumerate(tds):
                    val = td.get_text(strip=True)
                    if i in IDX_PERC:
                        val = val.replace("%", "").strip()
                    row.append(val)
                linhas.append(row)
        if not linhas:
            return None
        df = pd.DataFrame(linhas, columns=COLUNAS_SISVAN)
        df = df[~df["Municipio"].astype(str).str.contains("TOTAL", case=False, na=False)]
        if "Codigo_IBGE" in df.columns:
            df = df[df["Codigo_IBGE"].astype(str).str.match(r"^\d{6}$", na=False)]
        return df.reset_index(drop=True)
    except Exception as e:
        print(f"Erro ao processar HTML: {e}")
        return None


def main():
    """Faz uma requisição com o payload já construído e exibe no console os dados dos municípios."""
    print("Payload utilizado:")
    print("-" * 50)
    for k, v in sorted(PAYLOAD_BASE.items()):
        print(f"  {k}: {v}")
    print("-" * 50)

    session = requests.Session()
    try:
        session.get(URL_INDEX, headers=HEADERS, timeout=15)
    except Exception as e:
        print(f"Erro ao obter sessão: {e}")
        return

    try:
        response = session.post(URL_POST, data=PAYLOAD_BASE, headers=HEADERS, timeout=30)
    except Exception as e:
        print(f"Erro na requisição: {e}")
        return

    if response.status_code != 200:
        print(f"Erro: status {response.status_code}")
        return

    df = processar_html_para_dataframe(response.text)
    if df is None or df.empty:
        print("Nenhum dado de município retornado.")
        return

    print("\nValores retornados dos municípios:")
    print("=" * 80)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_rows", None)
    print(df.to_string(index=False))
    print("=" * 80)
    print(f"Total: {len(df)} municípios.")

    csv_path = "dados_sisvan_adulto.csv"
    try:
        salvar_csv(df, csv_path)
        print(f"\nCSV salvo: {csv_path}")
    except Exception as e:
        print(f"\nErro ao salvar CSV: {e}")


if __name__ == "__main__":
    main()