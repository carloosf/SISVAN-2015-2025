import basedosdados as bd

# Substitua pelo ID do seu projeto no Google Cloud
billing_id = "gen-lang-client-0526093869" 

query = """
SELECT 
    ano, 
    mes, 
    id_municipio, 
    fase_vida, 
    sexo, 
    raca_cor,
    peso, 
    altura, 
    imc,
    estado_nutricional_peso_idade_crianca,
    estado_nutricional_adulto,
    estado_nutricional_idoso
FROM `basedosdados.br_ms_sisvan.microdados` 
WHERE sigla_uf = 'PE' -- Filtro Geográfico
  AND ano BETWEEN 2015 AND 2023 -- Filtro de Partição (Economiza $$$)
"""

# O comando abaixo vai abrir o navegador para autenticar na primeira vez
df = bd.read_sql(query=query, billing_project_id=billing_id)

# Salva localmente para você não precisar rodar a query de novo
df.to_csv("microdados_sisvan_pe_2015_2023.csv", index=False, encoding='utf-8-sig')