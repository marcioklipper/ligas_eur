import pandas as pd
import numpy as np
import os
from github import Github
from io import StringIO
import cloudscraper # <--- A ferramenta nova
import time

# --- CONFIGURAÇÕES ---
GITHUB_TOKEN = os.getenv('GH_TOKEN')
NOME_REPO = "marcioklipper/ligas_eur"
ARQUIVO_JOGOS = "base_europa_unificada.csv"
ARQUIVO_FORCA = "forca_times.csv"

# URLs do FBref
urls_ligas = {
    'Premier League': {'Url': 'https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures', 'Pais': 'Inglaterra'},
    'La Liga':        {'Url': 'https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures',       'Pais': 'Espanha'},
    'Serie A':        {'Url': 'https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures',        'Pais': 'Italia'},
    'Bundesliga':     {'Url': 'https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures',     'Pais': 'Alemanha'},
    'Ligue 1':        {'Url': 'https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures',        'Pais': 'Franca'},
    'Primeira Liga':  {'Url': 'https://fbref.com/en/comps/32/schedule/Primeira-Liga-Scores-and-Fixtures',  'Pais': 'Portugal'},
    'Eredivisie':     {'Url': 'https://fbref.com/en/comps/23/schedule/Eredivisie-Scores-and-Fixtures',     'Pais': 'Holanda'}
}

# --- FUNÇÃO 1: BAIXAR JOGOS (COM ANTI-BLOQUEIO) ---
def atualizar_jogos():
    print("--- INICIANDO EXTRAÇÃO DE JOGOS (MODE: CLOUDSCRAPER) ---")
    dfs = []
    
    # Cria o navegador falso
    scraper = cloudscraper.create_scraper()

    for liga, info in urls_ligas.items():
        try:
            print(f"Lendo: {liga}...")
            
            # Usa o scraper em vez do requests normal
            response = scraper.get(info['Url'])
            
            # Se der erro de acesso, avisa
            if response.status_code != 200:
                print(f"Erro HTTP {response.status_code} em {liga}")
                continue

            tabelas = pd.read_html(StringIO(response.text))
            
            # Geralmente a tabela de jogos é a primeira
            df = tabelas[0]
            
            # Limpeza básica
            df = df[df['Wk'].ne('Wk')].copy() # Remove cabeçalhos repetidos
            
            # Garante colunas essenciais
            cols_map = {'Date': 'Data', 'Time': 'Hora', 'Home': 'Mandante', 'Away': 'Visitante', 'Score': 'Score'}
            # Filtra só o que existe
            df = df.rename(columns=cols_map)
            cols_finais = [c for c in ['Data', 'Hora', 'Mandante', 'Score', 'Visitante'] if c in df.columns]
            df = df[cols_finais]
            
            # Tratamento Placar (Separar Gols)
            if 'Score' in df.columns:
                def get_goals(score, idx):
                    if pd.isna(score) or score == "": return None
                    try: return int(str(score).split('–')[idx])
                    except: return None

                df['Gols_Mandante'] = df['Score'].apply(lambda x: get_goals(x, 0))
                df['Gols_Visitante'] = df['Score'].apply(lambda x: get_goals(x, 1))
                df = df.drop(columns=['Score'])
            else:
                df['Gols_Mandante'] = None
                df['Gols_Visitante'] = None
            
            # Resultado (H/D/A)
            def get_res(row):
                if pd.isna(row['Gols_Mandante']): return None
                if row['Gols_Mandante'] > row['Gols_Visitante']: return 'H'
                if row['Gols_Mandante'] < row['Gols_Visitante']: return 'A'
                return 'D'

            df['Resultado_Letra'] = df.apply(get_res, axis=1)
            df['Liga'] = liga
            df['Pais'] = info['Pais']
            
            # Remove jogos sem data (adiados indefinidamente)
            df = df.dropna(subset=['Data'])
            
            dfs.append(df)
            
            # Pausa de 5s para não irritar o servidor
            time.sleep(5)
            
        except Exception as e:
            print(f"Erro em {liga}: {e}")

    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        # Garante formato de data
        try:
            df_final['Data'] = pd.to_datetime(df_final['Data'])
        except:
            pass
        return df_final
    return None

# --- FUNÇÃO 2: CALCULAR FORÇA (IGUAL AO ANTERIOR) ---
def calcular_forca(df):
    print("--- CALCULANDO FORÇA DOS TIMES ---")
    
    # Filtra apenas jogos realizados
    df_jogos = df.dropna(subset=['Gols_Mandante', 'Gols_Visitante']).copy()
    
    if df_jogos.empty:
        print("Aviso: Nenhum jogo realizado encontrado para cálculo de força.")
        return pd.DataFrame()

    # Conversão para números
    df_jogos['Gols_Mandante'] = pd.to_numeric(df_jogos['Gols_Mandante'])
    df_jogos['Gols_Visitante'] = pd.to_numeric(df_jogos['Gols_Visitante'])

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

    # 4. Cálculo (com proteção contra divisão por zero)
    forca['Ataque_Casa'] = forca['Media_Feitos_Casa'] / forca['Media_Liga_Mandante'].replace(0, 1)
    forca['Defesa_Casa'] = forca['Media_Sofridos_Casa'] / forca['Media_Liga_Visitante'].replace(0, 1)
    forca['Ataque_Fora'] = forca['Media_Feitos_Fora'] / forca['Media_Liga_Visitante'].replace(0, 1)
    forca['Defesa_Fora'] = forca['Media_Sofridos_Fora'] / forca['Media_Liga_Mandante'].replace(0, 1)

    cols = ['Liga', 'Time', 'Ataque_Casa', 'Defesa_Casa', 'Ataque_Fora', 'Defesa_Fora']
    return forca[cols].round(4)

# --- EXECUÇÃO ---
def main():
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(NOME_REPO)

    # 1. Baixar Jogos
    df_jogos = atualizar_jogos()
    
    if df_jogos is not None and not df_jogos.empty:
        print(f"Sucesso! {len(df_jogos)} jogos extraídos.")
        
        # Salva Jogos
        csv_jogos = df_jogos.to_csv(index=False)
        try:
            contents = repo.get_contents(ARQUIVO_JOGOS)
            repo.update_file(contents.path, "Atualizando Jogos via Cloudscraper", csv_jogos, contents.sha)
            print("Jogos salvos no GitHub.")
        except:
            repo.create_file(ARQUIVO_JOGOS, "Criando Jogos via Cloudscraper", csv_jogos)
            print("Jogos criados no GitHub.")

        # 2. Calcular Força
        df_forca = calcular_forca(df_jogos)
        if not df_forca.empty:
            csv_forca = df_forca.to_csv(index=False)
            try:
                contents = repo.get_contents(ARQUIVO_FORCA)
                repo.update_file(contents.path, "Atualizando Forças", csv_forca, contents.sha)
                print("Forças salvas no GitHub.")
            except:
                repo.create_file(ARQUIVO_FORCA, "Criando Forças", csv_forca)
                print("Forças criadas no GitHub.")
    else:
        print("Falha: Nenhum dado foi extraído. O bloqueio do site pode estar muito forte.")

if __name__ == "__main__":
    main()
