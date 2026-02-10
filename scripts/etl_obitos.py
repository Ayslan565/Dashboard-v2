import pandas as pd
import os
import unicodedata
import re
import math
import time
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Integer
from concurrent.futures import ProcessPoolExecutor

# --- CONFIGURAÇÃO ---
DB_URL = 'mysql+pymysql://root:Jjjb3509@127.0.0.1:3306/db_pnatrans'

try:
    engine_principal = create_engine(DB_URL, pool_pre_ping=True)
except Exception as e:
    print(f"Erro BD: {e}")

def worker_salvar_chunk(dados_chunk):
    if dados_chunk.empty: return
    try:
        engine_worker = create_engine(DB_URL, pool_pre_ping=True)
        with engine_worker.connect() as conn:
            dados_chunk.to_sql('obitos_transporte', con=conn, if_exists='append', index=False, chunksize=1000)
    except Exception as e:
        print(f"  [Erro Worker] {e}")

# --- LIMPEZA DE NOMES DE COLUNA ---
def remover_acentos(texto):
    if not isinstance(texto, str): return str(texto)
    nfkd = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd if not unicodedata.combining(c)])

def limpar_header(col):
    """
    Transforma: 'local (uid)' -> 'local_uid'
    Transforma: 'MarÃ§o' -> 'marco'
    """
    c = col.lower().strip()
    c = remover_acentos(c)
    
    # Substituições específicas do seu arquivo
    c = c.replace(' (uid)', '_uid')
    c = c.replace(' (nome)', '_nome')
    c = c.replace(' ', '_')
    
    # Meses
    mapa_meses = {
        'jan': 'janeiro', 'fev': 'fevereiro', 'mar': 'marco', 'abr': 'abril',
        'mai': 'maio', 'jun': 'junho', 'jul': 'julho', 'ago': 'agosto',
        'set': 'setembro', 'out': 'outubro', 'nov': 'novembro', 'dez': 'dezembro'
    }
    
    for k, v in mapa_meses.items():
        if c.startswith(k): return v
        
    return c

def normalizar_colunas(df):
    novas_colunas = [limpar_header(c) for c in df.columns]
    df.columns = novas_colunas
    
    # Garante que 'total' vire 'total_anual' se vier só 'ano' ou 'total' no final
    if 'total' in df.columns: df.rename(columns={'total': 'total_anual'}, inplace=True)
    # Se tiver duas colunas 'ano', o pandas coloca 'ano.1'. A gente ajusta.
    cols = []
    for c in df.columns:
        if c == 'ano.1': cols.append('total_anual') # As vezes o total vem com nome de Ano
        else: cols.append(c)
    df.columns = cols
    
    return df.loc[:, ~df.columns.duplicated()]

# --- PROCESSAMENTO ---
def processar_obitos(PLANILHAS):
    # Procura qualquer arquivo com 'localidade' ou 'obitos'
    arquivos = [f for f in os.listdir(PLANILHAS) if ('localidade' in f.lower() or 'obito' in f.lower()) and f.endswith('.csv')]
    
    lista_dfs = []
    print(f"\n--- PROCESSANDO {len(arquivos)} ARQUIVOS ---")
    
    for arq in arquivos:
        caminho = os.path.join(PLANILHAS, arq)
        try:
            try: df = pd.read_csv(caminho, encoding='utf-8', sep=';', low_memory=False)
            except: df = pd.read_csv(caminho, encoding='latin-1', sep=';', low_memory=False)
            
            # Normaliza os cabeçalhos novos (uid/nome)
            df = normalizar_colunas(df)
            
            # Garante que meses sejam números
            meses = ['janeiro', 'fevereiro', 'marco', 'abril', 'maio', 'junho', 
                     'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']
            
            for mes in meses:
                if mes not in df.columns: df[mes] = 0
                else:
                    df[mes] = df[mes].astype(str).str.replace('.', '', regex=False).replace('-', '0')
                    df[mes] = pd.to_numeric(df[mes], errors='coerce').fillna(0).astype(int)

            # Preenche textos vazios
            cols_text = [c for c in df.columns if c not in meses and 'uid' not in c]
            for c in cols_text:
                df[c] = df[c].astype(str).replace('nan', 'NI')

            # Trata UIDs como Inteiros (se possível) ou String
            cols_uid = [c for c in df.columns if 'uid' in c]
            for c in cols_uid:
                df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).astype(int)

            print(f"  ✓ {arq}: {len(df):,} linhas.")
            lista_dfs.append(df)
        except Exception as e:
            print(f"  ERRO {arq}: {e}")

    if lista_dfs: return pd.concat(lista_dfs, ignore_index=True)
    return pd.DataFrame()

def salvar_banco(df):
    if df.empty: return
    
    # Remove colunas duplicadas ou estranhas
    df = df.loc[:, ~df.columns.duplicated()]
    
    print(f"\n--- SALVANDO NO BANCO ({len(df):,} linhas) ---")
    
    try:
        with engine_principal.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS obitos_transporte"))
            
            # CRIA A TABELA COM AS COLUNAS NOVAS (UID/NOME)
            sql = """
            CREATE TABLE obitos_transporte (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ano_uid INT, ano_nome VARCHAR(20),
                local_uid INT, local_nome VARCHAR(100),
                indicador_uid INT, indicador_nome VARCHAR(255),
                categoria_uid INT, categoria_nome VARCHAR(100),
                estatistica_uid INT, estatistica_nome VARCHAR(100),
                lococor_uid INT, lococor_nome VARCHAR(100),
                atestante_uid INT, atestante_nome VARCHAR(100),
                grupoetario_uid INT, grupoetario_nome VARCHAR(100),
                racacor_uid INT, racacor_nome VARCHAR(100),
                sexo_uid INT, sexo_nome VARCHAR(50),
                abrangencia_uid INT, abrangencia_nome VARCHAR(50),
                localidade_uid INT, localidade_nome VARCHAR(150),
                janeiro INT, fevereiro INT, marco INT, abril INT, maio INT, junho INT,
                julho INT, agosto INT, setembro INT, outubro INT, novembro INT, dezembro INT,
                total_anual INT
            );
            """
            conn.execute(text(sql))
            conn.commit()
        
        # Mapeia colunas do DF para o Banco (Ignora o que não bater)
        cols_validas = [
            'ano_uid', 'ano_nome', 'local_uid', 'local_nome', 
            'indicador_uid', 'indicador_nome', 'categoria_uid', 'categoria_nome',
            'estatistica_uid', 'estatistica_nome', 'lococor_uid', 'lococor_nome',
            'atestante_uid', 'atestante_nome', 'grupoetario_uid', 'grupoetario_nome',
            'racacor_uid', 'racacor_nome', 'sexo_uid', 'sexo_nome',
            'abrangencia_uid', 'abrangencia_nome', 'localidade_uid', 'localidade_nome',
            'janeiro', 'fevereiro', 'marco', 'abril', 'maio', 'junho',
            'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'
        ]
        
        # Adiciona total_anual se existir, senão calcula
        if 'total_anual' in df.columns:
            cols_validas.append('total_anual')
        
        # Filtra colunas
        final_cols = [c for c in cols_validas if c in df.columns]
        df_final = df[final_cols]
        
        # Salva
        num_workers = max(1, os.cpu_count() - 1)
        chunk = math.ceil(len(df_final) / num_workers)
        chunks = [df_final[i:i + chunk] for i in range(0, len(df_final), chunk)]
        
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            list(executor.map(worker_salvar_chunk, chunks))
            
        print("  ✓ SUCESSO! Tabela atualizada com estrutura UID/NOME.")

    except Exception as e:
        print(f"  ERRO CRÍTICO: {e}")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    PLANILHAS = os.path.join(os.path.dirname(BASE_DIR), 'Planilhas')
    
    df = processar_obitos(PLANILHAS)
    salvar_banco(df)
    