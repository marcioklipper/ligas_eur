import pandas as pd
import numpy as np
import os
from github import Github
from io import StringIO
import requests
from datetime import datetime

# CONFIGURAÇÕES
GITHUB_TOKEN = os.getenv('GH_TOKEN')
NOME_REPO = "marcioklipper/ligas_eur"
ARQUIVO_JOGOS = "base_europa_unificada.csv"

# URLs ESPN
urls_ligas = {
    'Premier League': 'https://www.espn.com.br/futebol/calendario/_/liga/ENG.1',
    'La Liga': 'https://www.espn.com.br/futebol/calendario/_/liga/ESP.1',
    'Serie A': 'https://www.espn.com.br/futebol/calendario/_/liga/ITA.1',
    'Bundesliga': 'https://www.espn.com.br/futebol/calendario/_/liga/GER.1',
    'Ligue 1': 'https://www.espn.com.br/futebol/calendario/_/liga/FRA.1',
    'Primeira Liga': 'https://www.espn.com.br/futebol/calendario/_/liga/POR.1',
    'Eredivisie': 'https://www.espn.com.br/futebol/calendario/_/liga/NED.1'
}

def main():
    print("--- EXTRAÇÃO ESPN ---")
    lista_dfs = []
    headers = {'User-Agent': 'Mozilla/5.0'}

    for liga, url in urls_ligas.items():
        print(f"Lendo: {liga}...")
        try:
            response = requests.get(url, headers=headers)
            # Tenta ler tabelas com fallback de parsers
            tabelas = pd.read_html(StringIO(response.text), flavor=['lxml', 'bs4'])
            
            for df in tabelas:
                if len(df) < 2: continue
                
                # Procura coluna 'vs'
                col_partida = next((c for c in df.columns if df[c].astype(str).str.contains(' vs ', na=False).any()), None)
                
                df_temp = df.copy()
                if col_partida:
                    try:
                        div = df_temp[col_partida].str.split(' vs ', expand=True)
                        if len(div.columns) >= 2:
                            df_temp['Mandante'] = div[0].str.strip()
                            df_temp['Visitante'] = div[1].str.strip()
                        else: continue
                    except: continue
                elif len(df.columns) >= 4:
                    df_temp['Mandante'] = df_temp.iloc[:, 0]
                    df_temp['Visitante'] = df_temp.iloc[:, 1]
                else: continue
                
                # Monta dataframe final
                df_temp['Liga'] = liga
                df_temp['Data'] = datetime.today().strftime('%Y-%m-%d')
                df_temp['Gols_Mandante'] = np.nan
                df_temp['Gols_Visitante'] = np.nan
                df_temp['Resultado_Letra'] = np.nan
                
                # Seleciona colunas
                cols = ['Data', 'Mandante', 'Visitante', 'Gols_Mandante', 'Gols_Visitante', 'Resultado_Letra', 'Liga']
                for c in cols: 
                    if c not in df_temp.columns: df_temp[c] = np.nan
                
                lista_dfs.append(df_temp[cols])
        except Exception as e:
            print(f"Erro {liga}: {e}")

    # Salvar no GitHub
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(NOME_REPO)
    
    # Baixar antigo
    try:
        url_raw = f"https://raw.githubusercontent.com/{NOME_REPO}/main/{ARQUIVO_JOGOS}"
        df_antigo = pd.read_csv(url_raw)
    except: df_antigo = pd.DataFrame()

    # Unir
    if lista_dfs:
        df_novos = pd.concat(lista_dfs, ignore_index=True)
        # Limpa logos
        for c in ['Mandante', 'Visitante']:
            df_novos[c] = df_novos[c].astype(str).str.replace(' logo', '', regex=False)
            
        df_final = pd.concat([df_antigo, df_novos], ignore_index=True)
    else:
        df_final = df_antigo

    # Commit
    if not df_final.empty:
        try:
            repo.update_file(ARQUIVO_JOGOS, "Update ESPN", df_final.to_csv(index=False), repo.get_contents(ARQUIVO_JOGOS).sha)
            print("Atualizado!")
        except:
            repo.create_file(ARQUIVO_JOGOS, "Create ESPN", df_final.to_csv(index=False))
            print("Criado!")

if __name__ == "__main__":
    main()
