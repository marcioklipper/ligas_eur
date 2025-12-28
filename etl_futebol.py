import pandas as pd
import os
from github import Github

# --- CONFIGURAÇÕES ---
GITHUB_TOKEN = os.getenv('GH_TOKEN') 
NOME_REPO = "marcioklipper/ligas_eur"  # JÁ ATUALIZADO PARA O SEU REPO
NOME_ARQUIVO_FINAL = "base_europa_unificada.csv"
TEMPORADA = "2526"

# Dicionário de Ligas
ligas = {
    'E0': {'Pais': 'Inglaterra', 'Liga': 'Premier League'},
    'SP1': {'Pais': 'Espanha',    'Liga': 'La Liga'},
    'N1':  {'Pais': 'Holanda',    'Liga': 'Eredivisie'},
    'I1':  {'Pais': 'Italia',     'Liga': 'Serie A'},
    'D1':  {'Pais': 'Alemanha',   'Liga': 'Bundesliga'},
    'P1':  {'Pais': 'Portugal',   'Liga': 'Liga Portugal'}
}

print("--- INICIANDO ROBÔ DE ATUALIZAÇÃO ---")

dfs = []
base_url = f"https://www.football-data.co.uk/mmz4281/{TEMPORADA}/"

# 1. DOWNLOAD E TRATAMENTO
for codigo, info in ligas.items():
    try:
        url = base_url + codigo + ".csv"
        print(f"Baixando: {info['Liga']}...")
        
        df = pd.read_csv(url)
        
        # Padronização de colunas
        cols_padrao = ['Date', 'Time', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR']
        cols_existentes = [c for c in cols_padrao if c in df.columns]
        df = df[cols_existentes]
        
        # Criar colunas extras
        df['Liga'] = info['Liga']
        df['Pais'] = info['Pais']
        
        dfs.append(df)
        
    except Exception as e:
        print(f"Erro na liga {info['Liga']}: {e}")

if dfs:
    # 2. CONSOLIDAÇÃO
    df_final = pd.concat(dfs, ignore_index=True)
    
    # Renomear colunas
    df_final = df_final.rename(columns={
        'Date': 'Data', 'Time': 'Hora', 'HomeTeam': 'Mandante', 'AwayTeam': 'Visitante',
        'FTHG': 'Gols_Mandante', 'FTAG': 'Gols_Visitante', 'FTR': 'Resultado_Letra'
    })
    
    # Tratamentos Finais
    df_final['Data'] = pd.to_datetime(df_final['Data'], dayfirst=True)
    
    if 'Hora' in df_final.columns:
        df_final['Hora'] = df_final['Hora'].fillna('00:00')
    else:
        df_final['Hora'] = '00:00'

    # Salvar CSV na memória
    csv_content = df_final.to_csv(index=False)
    print(f"Dados processados! Total de linhas: {len(df_final)}")

    # 3. UPLOAD PARA O GITHUB
    try:
        g = Github(GITHUB_TOKEN)
        repo = g.get_repo(NOME_REPO)
        
        try:
            contents = repo.get_contents(NOME_ARQUIVO_FINAL)
            repo.update_file(contents.path, "Atualização Automática", csv_content, contents.sha)
            print("Arquivo ATUALIZADO com sucesso!")
        except:
            repo.create_file(NOME_ARQUIVO_FINAL, "Carga Inicial", csv_content)
            print("Arquivo CRIADO com sucesso!")
            
    except Exception as e:
        print(f"Erro no GitHub: {e}")
        
else:
    print("Nenhum dado encontrado.")
