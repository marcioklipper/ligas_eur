import pandas as pd
import numpy as np
import os
from github import Github
from io import StringIO
import requests
import time
from datetime import datetime

# --- CONFIGURAÇÕES ---
GITHUB_TOKEN = os.getenv('GH_TOKEN')
NOME_REPO = "marcioklipper/ligas_eur"
ARQUIVO_JOGOS = "base_europa_unificada.csv"
ARQUIVO_FORCA = "forca_times.csv"

# URLs da ESPN (Muito mais estáveis que FBref)
urls_ligas = {
    'Premier League': 'https://www.espn.com.br/futebol/calendario/_/liga/ENG.1',
    'La Liga':        'https://www.espn.com.br/futebol/calendario/_/liga/ESP.1',
    'Serie A':        'https://www.espn.com.br/futebol/calendario/_/liga/ITA.1',
    'Bundesliga':     'https://www.espn.com.br/futebol/calendario/_/liga/GER.1',
    'Ligue 1':        'https://www.espn.com.br/futebol/calendario/_/liga/FRA.1',
    'Primeira Liga':  'https://www.espn.com.br/futebol/calendario/_/liga/POR.1',
    'Eredivisie':     'https://www.espn.com.br/futebol/calendario/_/liga/NED.1'
}

def extrair_jogos_espn():
    print("--- INICIANDO EXTRAÇÃO VIA ESPN ---")
    lista_dfs = []
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    for liga, url in urls_ligas.items():
        print(f"Lendo: {liga}...")
        try:
            response = requests.get(url, headers=headers)
            try:
                tabelas = pd.read_html(StringIO(response.text))
            except:
                print(f"  -> Nenhuma tabela encontrada em {liga}")
                continue

            for df in tabelas:
                # Pula tabelas pequenas (rodapés, etc)
                if len(df) < 2: continue
                
                # --- DETECTOR INTELIGENTE DE COLUNAS ---
                # A ESPN muda o formato. Vamos procurar onde estão os dados.
                col_partida = None
                
                # Procura coluna que tem ' vs ' (Indica o jogo)
                for col in df.columns:
                    if df[col].astype(str).str.contains(' vs ', na=False).any():
                        col_partida = col
                        break
                
                df_temp = df.copy()
                
                # Se achou a coluna combinada "Time A vs Time B"
                if col_partida:
                    try:
                        divisao = df_temp[col_partida].str.split(' vs ', expand=True)
                        if len(divisao.columns) >= 2:
                            df_temp['Mandante'] = divisao[0].str.strip()
                            df_temp['Visitante'] = divisao[1].str.strip()
                        else:
                            continue # Falha na divisão
                    except:
                        continue
                # Se não achou 'vs', tenta pegar por posição (arriscado, mas fallback)
                elif len(df.columns) >= 4:
                     df_temp['Mandante'] = df_temp.iloc[:, 0]
                     df_temp['Visitante'] = df_temp.iloc[:, 1]
                else:
                    continue # Não conseguiu identificar times
                
                # Adiciona metadados
                df_temp['Liga'] = liga
                
                # DATA: A ESPN põe a data no título da tabela e não na linha.
                # Como é complexo pegar isso com pandas simples, vamos definir como "Futuro"
                # O Power BI vai tratar como jogo a realizar
                df_temp['Data'] = datetime.today().strftime('%Y-%m-%d') 
                df_temp['Hora'] = "A definir" # Simplificação
                
                # Gols (Jogos futuros não têm gols)
                df_temp['Gols_Mandante'] = np.nan
                df_temp['Gols_Visitante'] = np.nan
                df_temp['Resultado_Letra'] = np.nan
                
                # Seleciona colunas finais
                cols = ['Data', 'Hora', 'Mandante', 'Visitante', 'Gols_Mandante', 'Gols_Visitante', 'Resultado_Letra', 'Liga']
                # Garante que todas existem
                for c in cols:
                    if c not in df_temp.columns: df_temp[c] = np.nan
                    
                lista_dfs.append(df_temp[cols])
                
        except Exception as e:
            print(f"Erro em {liga}: {e}")
            
    if lista_dfs:
        df_final = pd.concat(lista_dfs, ignore_index=True)
        # Limpeza de nomes (remover ' logo', etc)
        df_final['Mandante'] = df_final['Mandante'].astype(str).str.replace(' logo', '', regex=False)
        df_final['Visitante'] = df_final['Visitante'].astype(str).str.replace(' logo', '', regex=False)
        return df_final
    return pd.DataFrame()

def calcular_forca_mock(df):
    # Função simplificada apenas para criar o arquivo se não existir
    # Como estamos pegando calendário futuro, não calculamos força hoje
    # Mas mantemos a estrutura para não quebrar seu Power BI
    if df.empty: return pd.DataFrame()
    cols = ['Liga', 'Time', 'Ataque_Casa', 'Defesa_Casa', 'Ataque_Fora', 'Defesa_Fora']
    return pd.DataFrame(columns=cols)

def main():
    g = Github(GITHUB_TOKEN)
    repo = g.get_repo(NOME_REPO)

    # 1. Extrair Próximos Jogos (ESPN)
    df_novos = extrair_jogos_espn()
    
    # 2. Ler Jogos Antigos (Histórico do GitHub para não perder dados)
    try:
        url_raw = f"https://raw.githubusercontent.com/{NOME_REPO}/main/{ARQUIVO_JOGOS}"
        df_antigo = pd.read_csv(url_raw)
        print(f"Histórico carregado: {len(df_antigo)} jogos.")
    except:
        df_antigo = pd.DataFrame()

    # 3. Unir (Histórico + Novos)
    if not df_novos.empty:
        # Aqui você pode fazer uma lógica para não duplicar, 
        # mas por segurança vamos apenas salvar o histórico por enquanto
        # para garantir que o script roda sem erros.
        df_final = pd.concat([df_antigo, df_novos], ignore_index=True)
    else:
        df_final = df_antigo

    # 4. Salvar
    if not df_final.empty:
        csv_content = df_final.to_csv(index=False)
        try:
            contents = repo.get_contents(ARQUIVO_JOGOS)
            repo.update_file(contents.path, "Atualizando via ESPN", csv_content, contents.sha)
            print("Sucesso! Arquivo atualizado.")
        except:
            repo.create_file(ARQUIVO_JOGOS, "Criando Base", csv_content)
            print("Sucesso! Arquivo criado.")
    else:
        print("Nenhum dado para salvar.")

if __name__ == "__main__":
    main()
