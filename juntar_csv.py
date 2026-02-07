import pandas as pd
import glob

# Busca todos os CSVs
arquivos_csv = glob.glob("*.csv")

if not arquivos_csv:
    print("Nenhum arquivo CSV encontrado.")
else:
    lista_df = []
    
    for f in arquivos_csv:
        print(f"Lendo: {f}")
        # Adicionei sep=';' ou sep=None com engine='python' para detectar automaticamente
        try:
            # Tenta ler com ponto e vírgula, que é o padrão comum em bases brasileiras
            df = pd.read_csv(f, sep=';', encoding='latin1', low_memory=False)
            lista_df.append(df)
        except Exception as e:
            print(f"Erro ao ler {f}: {e}")

    if lista_df:
        print("Concatenando arquivos...")
        df_final = pd.concat(lista_df, ignore_index=True)
        
        # Salva o resultado
        df_final.to_csv("combinado_sisvan.csv", index=False, sep=';', encoding='utf-8-sig')
        print("Arquivo 'combinado_sisvan.csv' gerado com sucesso!")