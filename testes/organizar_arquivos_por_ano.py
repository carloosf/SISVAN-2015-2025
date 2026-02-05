import os
import shutil
import glob

# Anos a processar
anos = list(range(2015, 2026))  # 2015 a 2025

print("=" * 80)
print("ORGANIZANDO ARQUIVOS POR ANO")
print("=" * 80)

arquivos_movidos = 0

for ano in anos:
    pasta_ano = str(ano)
    
    # Criar pasta do ano se não existir
    if not os.path.exists(pasta_ano):
        os.makedirs(pasta_ano)
        print(f"\nPasta criada: {pasta_ano}/")
    
    # Procurar CSVs do ano
    padrao_csv = f"sisvan_crianca_{ano}_*.csv"
    csvs = glob.glob(padrao_csv)
    
    # Procurar HTMLs do ano
    padrao_html = f"resposta_sisvan_{ano}_*.html"
    htmls = glob.glob(padrao_html)
    
    # Mover CSVs
    for csv in csvs:
        if os.path.isfile(csv):
            destino = os.path.join(pasta_ano, os.path.basename(csv))
            try:
                shutil.move(csv, destino)
                print(f"  Movido: {csv} -> {destino}")
                arquivos_movidos += 1
            except Exception as e:
                print(f"  ERRO ao mover {csv}: {e}")
    
    # Mover HTMLs
    for html in htmls:
        if os.path.isfile(html):
            destino = os.path.join(pasta_ano, os.path.basename(html))
            try:
                shutil.move(html, destino)
                print(f"  Movido: {html} -> {destino}")
                arquivos_movidos += 1
            except Exception as e:
                print(f"  ERRO ao mover {html}: {e}")

print("\n" + "=" * 80)
print(f"ORGANIZACAO CONCLUIDA!")
print(f"Total de arquivos movidos: {arquivos_movidos}")
print("=" * 80)

# Listar estrutura final
print("\nEstrutura de pastas criada:")
for ano in anos:
    pasta_ano = str(ano)
    if os.path.exists(pasta_ano):
        arquivos = [f for f in os.listdir(pasta_ano) if os.path.isfile(os.path.join(pasta_ano, f))]
        if arquivos:
            print(f"  {pasta_ano}/ ({len(arquivos)} arquivos)")

print("\nArquivos na raiz (devem permanecer apenas o CSV completo e o script):")
arquivos_raiz = [f for f in os.listdir('.') if os.path.isfile(f) and not f.endswith('.py') and not f == 'sisvan_crianca_completo.csv']
if arquivos_raiz:
    print(f"  AVISO: {len(arquivos_raiz)} arquivos ainda na raiz:")
    for arq in arquivos_raiz[:10]:  # Mostrar apenas os primeiros 10
        print(f"    - {arq}")
    if len(arquivos_raiz) > 10:
        print(f"    ... e mais {len(arquivos_raiz) - 10} arquivos")
else:
    print("  OK - Apenas arquivos essenciais na raiz")

print("=" * 80)

