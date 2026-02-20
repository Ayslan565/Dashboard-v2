import pandas as pd
import os
import unicodedata
import re
import math
from sqlalchemy import create_engine, text
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
            # MODO 'append' para adicionar sem apagar a tabela existente
            dados_chunk.to_sql('obitos_transporte', con=conn, if_exists='append', index=False, chunksize=1000)
    except Exception as e:
        print(f"  [Erro Worker] {e}")

# --- LIMPEZA DE NOMES DE COLUNA ---
def remover_acentos(texto):
    if not isinstance(texto, str): return str(texto)
    nfkd = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd if not unicodedata.combining(c)])

def limpar_header(col):
    c = col.lower().strip()
    c = remover_acentos(c)
    
    # Padronização específica
    c = c.replace(' (uid)', '_uid')
    c = c.replace(' (nome)', '_nome')
    c = c.replace(' ', '_')
    
    mapa_meses = {
        'jan': 'janeiro', 'fev': 'fevereiro', 'mar': 'marco', 'abr': 'abril',
        'mai': 'maio', 'jun': 'junho', 'jul': 'julho', 'ago': 'agosto',
        'set': 'setembro', 'out': 'outubro', 'nov': 'novembro', 'dez': 'dezembro'
    }
    
    for k, v in mapa_meses.items():
        # CORREÇÃO: Verifica de forma mais restrita para não confundir 'abr' de abrangencia com abril
        if c == k or c == v or c.startswith(f"{k}_") or c.startswith(f"{k}-") or c.startswith(f"{k}/"):
            return v
        
    return c

def normalizar_colunas(df):
    novas_colunas = [limpar_header(c) for c in df.columns]
    df.columns = novas_colunas
    
    cols_map = {}
    for col in df.columns:
        if col == 'ano': 
            cols_map[col] = 'total_anual'
        elif col == 'ano.1':
            cols_map[col] = 'total_anual'
            
    if cols_map:
        df.rename(columns=cols_map, inplace=True)
    
    # Remove colunas duplicadas
    df = df.loc[:, ~df.columns.duplicated()]
    return df

def validar_estrutura(df):
    """
    Verifica se a aba do Excel tem cara de dados brutos.
    Abas de resumo (com 'Soma de Ano', 'Vários itens') serão ignoradas.
    """
    # Converte colunas para minúsculo para verificar
    cols = [str(c).lower() for c in df.columns]
    
    # Critérios: Deve ter coluna de mês OU coluna de ano com UID
    tem_janeiro = any('jan' in c for c in cols)
    tem_ano_uid = any('ano' in c and 'uid' in c for c in cols)
    
    # Se não tiver nenhum desses, provavelmente é uma aba de resumo
    return tem_janeiro or tem_ano_uid

def tratar_dataframe(df, nome_origem):
    try:
        # 1. Validação: Ignora abas que não são de dados brutos
        if not validar_estrutura(df):
            print(f"    -> [Ignorado] Aba de resumo ou estrutura diferente: {nome_origem}")
            return pd.DataFrame()

        # 2. Normaliza cabeçalhos
        df = normalizar_colunas(df)
        
        # 3. Remove linhas de rodapé (lixo no final do arquivo)
        if 'ano_uid' in df.columns:
            df = df[pd.to_numeric(df['ano_uid'], errors='coerce').notna()]
        
        # 4. Limpa asteriscos nos anos (ex: "2025*")
        if 'ano_nome' in df.columns:
            df['ano_nome'] = df['ano_nome'].astype(str).str.replace('*', '', regex=False).str.strip()
        
        # 5. Tratamento de Meses e Total
        meses = ['janeiro', 'fevereiro', 'marco', 'abril', 'maio', 'junho', 
                 'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro', 'total_anual']
        
        for col in meses:
            if col not in df.columns: 
                if col != 'total_anual': df[col] = 0
            else:
                # Remove pontos de milhar e converte para inteiro
                df[col] = df[col].astype(str).str.replace('.', '', regex=False).replace('-', '0')
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        # 6. Preenche textos vazios
        cols_text = [c for c in df.columns if c not in meses and 'uid' not in c]
        for c in cols_text:
            df[c] = df[c].astype(str).replace('nan', 'NI').replace('None', 'NI')

        # 7. Garante UIDs como Inteiros
        cols_uid = [c for c in df.columns if 'uid' in c]
        for c in cols_uid:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).astype(int)
            
        print(f"    -> Processado: {len(df):,} linhas.")
        return df

    except Exception as e:
        print(f"    -> ERRO ao tratar {nome_origem}: {e}")
        return pd.DataFrame()

# --- PROCESSAMENTO PRINCIPAL ---
def processar_obitos(PLANILHAS):
    # Procura especificamente o seu arquivo Excel ou outros CSVs
    arquivos = [f for f in os.listdir(PLANILHAS) 
                if (('ms' in f.lower() or 'obito' in f.lower()) and 
                    (f.endswith('.xlsx') or f.endswith('.xls') or f.endswith('.csv')))]
    
    lista_dfs = []
    print(f"\n--- ENCONTRADOS {len(arquivos)} ARQUIVOS ---")
    
    for arq in arquivos:
        caminho = os.path.join(PLANILHAS, arq)
        print(f"\nArquivo: {arq}")
        
        try:
            # --- LÓGICA PARA EXCEL (.xlsx) ---
            if arq.endswith('.xlsx') or arq.endswith('.xls'):
                print("  Lendo todas as abas do Excel...")
                # sheet_name=None carrega TODAS as abas num dicionário
                xls = pd.read_excel(caminho, sheet_name=None)
                
                for nome_aba, df_aba in xls.items():
                    print(f"  > Aba '{nome_aba}': ", end="")
                    df_limpo = tratar_dataframe(df_aba, f"{arq}::{nome_aba}")
                    if not df_limpo.empty:
                        lista_dfs.append(df_limpo)

            # --- LÓGICA PARA CSV (caso exista algum solto) ---
            else:
                print("  Lendo CSV... ", end="")
                try: df = pd.read_csv(caminho, encoding='utf-8', sep=',', low_memory=False)
                except: 
                    try: df = pd.read_csv(caminho, encoding='latin-1', sep=';', low_memory=False)
                    except: df = pd.read_csv(caminho, encoding='utf-8', sep=';', low_memory=False)
                
                df_limpo = tratar_dataframe(df, arq)
                if not df_limpo.empty:
                    lista_dfs.append(df_limpo)
            
        except Exception as e:
            print(f"  FALHA NO ARQUIVO: {e}")

    if lista_dfs: return pd.concat(lista_dfs, ignore_index=True)
    return pd.DataFrame()

def salvar_banco(df):
    if df.empty: 
        print("  -> Nenhum dado válido encontrado para salvar.")
        return
    
    # Remove duplicatas gerais antes de salvar
    df = df.loc[:, ~df.columns.duplicated()]
    print(f"\n--- ADICIONANDO AO BANCO ({len(df):,} linhas totais) ---")
    
    try:
        with engine_principal.connect() as conn:
            # Cria a tabela SE NÃO EXISTIR (preserva dados antigos)
            sql = """
            CREATE TABLE IF NOT EXISTS obitos_transporte (
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
        
        # Filtra apenas colunas que existem na tabela
        cols_validas = [
            'ano_uid', 'ano_nome', 'local_uid', 'local_nome', 
            'indicador_uid', 'indicador_nome', 'categoria_uid', 'categoria_nome',
            'estatistica_uid', 'estatistica_nome', 'lococor_uid', 'lococor_nome',
            'atestante_uid', 'atestante_nome', 'grupoetario_uid', 'grupoetario_nome',
            'racacor_uid', 'racacor_nome', 'sexo_uid', 'sexo_nome',
            'abrangencia_uid', 'abrangencia_nome', 'localidade_uid', 'localidade_nome',
            'janeiro', 'fevereiro', 'marco', 'abril', 'maio', 'junho',
            'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro',
            'total_anual'
        ]
        
        final_cols = [c for c in cols_validas if c in df.columns]
        df_final = df[final_cols]
        
        # Salva em paralelo (Append)
        num_workers = max(1, os.cpu_count() - 1)
        chunk = math.ceil(len(df_final) / num_workers)
        chunks = [df_final[i:i + chunk] for i in range(0, len(df_final), chunk)]
        
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            list(executor.map(worker_salvar_chunk, chunks))
            
        print("  ✓ SUCESSO! Novos dados adicionados.")

    except Exception as e:
        print(f"  ERRO CRÍTICO NO SALVAMENTO: {e}")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    PLANILHAS = os.path.join(os.path.dirname(BASE_DIR), 'Planilhas')
    
    if not os.path.exists(PLANILHAS):
        print(f"ERRO: Pasta '{PLANILHAS}' não encontrada.")
    else:
        df = processar_obitos(PLANILHAS)
        salvar_banco(df)