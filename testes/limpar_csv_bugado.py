import pandas as pd
import os

# Nome do CSV
csv_file = "sisvan_crianca_completo.csv"

if os.path.exists(csv_file):
    print("=" * 80)
    print("LIMPANDO CSV BUGADO")
    print("=" * 80)
    
    # Ler CSV
    df = pd.read_csv(csv_file, sep=';', encoding='utf-8-sig')
    
    print(f"\nTotal de linhas antes: {len(df)}")
    
    # Remover linhas onde Município contém "TOTAL" ou é "-" ou está vazio
    df_limpo = df[
        (~df['Municipio'].astype(str).str.contains('TOTAL', case=False, na=False)) &
        (df['Municipio'].astype(str) != '-') &
        (df['Municipio'].astype(str).str.strip() != '') &
        (df['Municipio'].astype(str).str.strip() != 'nan')
    ]
    
    print(f"Total de linhas depois: {len(df_limpo)}")
    print(f"Linhas removidas: {len(df) - len(df_limpo)}")
    
    # Verificar se há linhas com dados válidos
    if len(df_limpo) > 0:
        # Verificar se as colunas numéricas estão corretas
        # Se Regiao contém "TOTAL BRASIL", significa que os dados estão mapeados incorretamente
        linhas_incorretas = df_limpo[df_limpo['Regiao'].astype(str).str.contains('TOTAL BRASIL', case=False, na=False)]
        
        if len(linhas_incorretas) > 0:
            print(f"\nAVISO: {len(linhas_incorretas)} linhas ainda contem 'TOTAL BRASIL' na coluna Regiao")
            print("Essas linhas provavelmente tem dados mapeados incorretamente.")
            print("Removendo essas linhas...")
            
            df_limpo = df_limpo[
                ~df_limpo['Regiao'].astype(str).str.contains('TOTAL BRASIL', case=False, na=False)
            ]
            
            print(f"Total de linhas apos remover incorretas: {len(df_limpo)}")
        
        # Salvar CSV limpo
        backup_file = csv_file.replace('.csv', '_backup_bugado.csv')
        print(f"\nFazendo backup do CSV original: {backup_file}")
        df.to_csv(backup_file, index=False, encoding='utf-8-sig', sep=';')
        
        print(f"Salvando CSV limpo: {csv_file}")
        df_limpo.to_csv(csv_file, index=False, encoding='utf-8-sig', sep=';')
        
        print("\n" + "=" * 80)
        print("CSV LIMPO COM SUCESSO!")
        print("=" * 80)
        print(f"\nArquivo original salvo como: {backup_file}")
        print(f"Arquivo limpo salvo como: {csv_file}")
        print(f"\nEstatisticas do CSV limpo:")
        print(f"  Total de registros: {len(df_limpo)}")
        print(f"  Anos: {sorted(df_limpo['Ano'].unique().tolist())}")
        print(f"  Fases de vida: {sorted(df_limpo['fase_vida'].unique().tolist())}")
        
        # Mostrar exemplo de uma linha válida
        if len(df_limpo) > 0:
            print(f"\nExemplo de linha valida:")
            print(df_limpo.iloc[0][['Ano', 'fase_vida', 'sexo', 'raca_cor', 'Municipio', 'Total']].to_string())
    else:
        print("\nAVISO: Nenhuma linha valida encontrada apos limpeza!")
        print("O CSV sera mantido como esta, mas nao ha dados validos.")
    
    print("\n" + "=" * 80)
else:
    print(f"Arquivo {csv_file} nao encontrado!")

