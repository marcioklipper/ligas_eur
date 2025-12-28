import pandas as pd
import numpy as np
import os
from github import Github
from io import StringIO
import requests
import time

# --- CONFIGURAÇÕES ---
GITHUB_TOKEN = os.getenv('GH_TOKEN')
NOME_REPO = "marcioklipper/ligas_eur"  # Seu repositório
ARQUIVO_JOGOS = "base_europa_unificada.csv" # Arquivo que já existe (NÃO SERÁ ESTRAGADO)
ARQUIVO_FORCA = "forca_times.csv"           # Arquivo novo (Tabela Auxiliar)

# URLs do FBref (Mantendo sua extração original de jogos)
urls_ligas = {
    'Premier League': {'Url': 'https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures', 'Pais': 'Inglaterra'},
    'La Liga':        {'Url': 'https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures',       'Pais': 'Espanha'},
    'Serie A':        {'Url': 'https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures',        'Pais': 'Italia'},
    'Bundesliga':     {'Url': 'https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures',     'Pais': 'Alemanha'},
    'Ligue 1':        {'Url': 'https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures',        'Pais': 'Franca'},
    'Primeira Liga':  {'Url': 'https://fbref.com/en/comps/32/schedule/Primeira-Liga-Scores-and-Fixtures',  'Pais': 'Portugal'},
    'Eredivisie':     {'Url': 'https://fbref.com/en/comps/23/schedule/Eredivisie-Scores-and-Fixtures',     'Pais': 'Holanda'}
}

# --- FUNÇÃO 1: BAIXAR JOGOS (ETL Padrão) ---
def atualizar_jogos():
    print("--- INICIANDO EXTRAÇÃO DE JOGOS ---")
    dfs = []
    
    # Headers para evitar erro 403
    headers = {'User-Agent': 'Mozilla/5.0'}

    for liga, info in urls_ligas.items():
        try:
            print(f"Lendo: {liga}...")
            # Usa requests para evitar bloqueio simples
            response = requests.get(info['Url'], headers=headers)
            tabelas = pd.read_html(StringIO(response.text))
            df = tabelas[0]
            
            # Limpeza básica (igual ao seu original)
            df = df[df['Wk'].ne('Wk')].copy()
            df = df[['Date', 'Time', 'Home', 'Score', 'Away']].copy()
            df.columns = ['Data', 'Hora', 'Mandante', 'Score', 'Visitante']
            
            # Tratamento Placar
            def get_goals(score, idx):
                if pd.isna(score) or score == "": return None
                try: return int(score.split('–')[idx])
                except: return None

            df['Gols_Mandante'] = df['Score'].apply(lambda x: get_goals(x, 0))
            df['Gols_Visitante'] = df['Score'].apply(lambda x: get_goals(x, 1))
            
            # Resultado (H/D/A)
            def get_res(row):
                if pd.isna(row['Gols_Mandante']): return None
                if row['Gols_Mandante'] > row['Gols_Visitante']: return 'H'
                if row['Gols_Mandante'] < row['Gols_Visitante']: return 'A'
                return 'D'

            df['Resultado_Letra'] = df.apply(get_res, axis=1)
            df['Liga'] = liga
            df['Pais'] = info['Pais']
            df = df.drop(columns=['Score'])
            df = df.dropna(subset=['Data'])
            
            dfs.append(df)
            time.sleep(3)
        except Exception as e:
            print(f"Erro em {liga}: {e}")

    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        df_final['Data'] = pd.to_datetime(df_final['Data'])
        return df_final
    return None

# --- FUNÇÃO 2: CALCULAR FORÇA (A Mágica da Previsão) ---
def calcular_forca(df):
    print("--- CALCULANDO FORÇA DOS TIMES (POISSON) ---")
    
    # Filtra apenas jogos que já aconteceram (têm gols)
    df_jogos = df.dropna(subset=['Gols_Mandante', 'Gols_Visitante']).copy()

    # 1. Médias da Liga
    medias_liga = df_jogos.groupby('Liga').agg(
        Media_Liga_Mandante=('Gols_Mandante', 'mean'),
        Media_Liga_Visitante=('Gols_Visitante', 'mean')
    ).reset_index()

    # 2. Médias dos Times
    stats_casa = df_jogos.groupby(['Liga', 'Mandante']).agg(
        Media_Feitos_Casa=('Gols_Mandante', 'mean'),
        Media_Sofridos_Casa=('Gols_Visitante', 'mean')
    ).reset_index().rename(columns={'Mandante': 'Time'})

    stats_fora = df_jogos.groupby(['Liga', 'Visitante']).agg(
        Media_Feitos_Fora=('Gols_Visitante', 'mean'),
        Media_Sofridos_Fora=('Gols_Mandante', 'mean')
    ).reset_index().rename(columns={'Visitante': 'Time'})

    # 3. Consolidação
    forca = pd.merge(stats_casa, stats_fora, on=['Liga', 'Time'])
    forca = pd.merge(forca, medias_liga, on='Liga')

    # 4. Cálculo dos Índices
    # Evitar divisão por zero usando np.maximum
    forca['Ataque_Casa'] = forca['Media_Feitos_Casa'] / forca['Media_Liga_Mandante']
    forca['Defesa_Casa'] = forca['Media_Sofridos_Casa'] / forca['Media_Liga_Visitante']
    forca['Ataque_Fora'] = forca['Media_Feitos_Fora'] / forca['Media_Liga_Visitante']
    forca['Defesa_Fora'] = forca['Media_Sofridos_Fora'] / forca['Media_Liga_Mandante']

    return forca[['Liga', 'Time', 'Ataque_Casa', 'Defesa_Casa', 'Ataque_Fora', 'Defesa_Fora']].round(4)

# --- EXECUÇÃO PRINCIPAL ---
def main():
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(NOME_REPO)

    # 1. Gerar Base de Jogos Atualizada
    df_jogos = atualizar_jogos()
    
    if df_jogos is not None:
        # Salva o arquivo de Jogos (Isso mantém seu processo atual funcionando)
        csv_jogos = df_jogos.to_csv(index=False)
        try:
            contents = repo.get_contents(ARQUIVO_JOGOS)
            repo.update_file(contents.path, "Atualizando Jogos", csv_jogos, contents.sha)
            print("Base de Jogos atualizada!")
        except:
            repo.create_file(ARQUIVO_JOGOS, "Criando Base Jogos", csv_jogos)
            print("Base de Jogos criada!")

        # 2. Gerar Tabela de Força (NOVO ARQUIVO)
        df_forca = calcular_forca(df_jogos)
        csv_forca = df_forca.to_csv(index=False)
        
        try:
            contents = repo.get_contents(ARQUIVO_FORCA)
            repo.update_file(contents.path, "Atualizando Forças", csv_forca, contents.sha)
            print(f"Arquivo '{ARQUIVO_FORCA}' atualizado com sucesso!")
        except:
            repo.create_file(ARQUIVO_FORCA, "Criando Tabela Força", csv_forca)
            print(f"Arquivo '{ARQUIVO_FORCA}' criado com sucesso!")

if __name__ == "__main__":
    main()
