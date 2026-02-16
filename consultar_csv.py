"""
Consulta dados de um município no CSV gerado pelo ETL (dados_sisvan_adulto.csv).
"""
import pandas as pd
from pathlib import Path

CSV_PATH = Path(__file__).parent / "dados_sisvan_adulto.csv"


def carregar_csv():
    """Carrega o CSV com separador ; e encoding UTF-8."""
    if not CSV_PATH.exists():
        print(f"Arquivo não encontrado: {CSV_PATH}")
        print("Execute primeiro o ETL.py para gerar o CSV.")
        return None
    return pd.read_csv(CSV_PATH, sep=";", encoding="utf-8-sig")


def listar_municipios(df: pd.DataFrame):
    """Lista todos os municípios disponíveis (nome e código IBGE)."""
    if "Municipio" not in df.columns:
        print("CSV não possui coluna 'Municipio'.")
        return
    municipios = df[["Codigo_IBGE", "Municipio"]].drop_duplicates().sort_values("Municipio")
    for _, row in municipios.iterrows():
        print(f"  {row['Codigo_IBGE']} - {row['Municipio']}")


def consultar_municipio(df: pd.DataFrame, nome: str):
    """Filtra e exibe as linhas do município (busca parcial, case insensitive)."""
    col = "Municipio"
    if col not in df.columns:
        print("CSV não possui coluna 'Municipio'.")
        return
    mask = df[col].astype(str).str.upper().str.contains(nome.upper().strip(), na=False)
    resultado = df[mask]
    if resultado.empty:
        print(f"Nenhum município encontrado para: {nome}")
        return
    print(f"\n--- {len(resultado)} registro(s) para '{nome}' ---\n")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    print(resultado.to_string(index=False))


def main():
    df = carregar_csv()
    if df is None:
        return

    print("Consulta CSV SISVAN - Adulto/Idoso (IMC)")
    print("=" * 50)
    print("\n1 - Listar todos os municípios")
    print("2 - Pesquisar município por nome")
    print("0 - Sair")
    opcao = input("\nOpção: ").strip()

    if opcao == "1":
        print("\nMunicípios no CSV:")
        listar_municipios(df)
    elif opcao == "2":
        nome = input("Nome do município (ou parte do nome): ").strip()
        if nome:
            consultar_municipio(df, nome)
        else:
            print("Digite um nome para pesquisar.")
    elif opcao != "0":
        print("Opção inválida.")


if __name__ == "__main__":
    main()
