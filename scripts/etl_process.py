import pandas as pd
import os
import unicodedata
import re
import math
import time
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Integer, Text, Float, Date
from concurrent.futures import ProcessPoolExecutor

# --- CONFIGURAÇÃO GLOBAL ---
DB_URL = 'mysql+pymysql://root:Jjjb3509@127.0.0.1:3306/db_pnatrans'

try:
    # pool_pre_ping mantém a conexão viva e evita quedas
    engine_principal = create_engine(DB_URL, pool_pre_ping=True)
except Exception as e:
    print(f"Erro Crítico na configuração do banco: {e}")

# --- WORKER PARALELO (PROCESSAMENTO RÁPIDO) ---
def worker_salvar_chunk(dados_chunk):
    """Salva um pedaço do dataframe usando uma nova conexão para paralelismo."""
    if dados_chunk.empty: return
    try:
        engine_worker = create_engine(DB_URL, pool_pre_ping=True)
        with engine_worker.connect() as conn:
            dados_chunk.to_sql('acidentes_prf', con=conn, if_exists='append', index=False, chunksize=1000)
    except Exception as e:
        print(f"  [Erro Worker] Falha ao salvar lote: {e}")

# --- SALVAMENTO SEGURO (TABELAS PEQUENAS) ---
def salvar_tabela_segura(df, nome_tabela):
    """Salva tabelas de gestão (Produtos, Órgãos) com segurança."""
    if df is None or df.empty: return
    try:
        df = df.loc[:, ~df.columns.duplicated()]
        with engine_principal.connect() as conn:
            with conn.begin():
                df.to_sql(nome_tabela, con=conn, if_exists='replace', index=False)
        print(f"  -> Tabela '{nome_tabela}' salva com sucesso.")
    except Exception as e:
        print(f"  ERRO ao salvar '{nome_tabela}': {e}")

# --- FUNÇÕES DE LIMPEZA ---
def remover_acentos(texto):
    if not isinstance(texto, str): return str(texto)
    nfkd = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd if not unicodedata.combining(c)])

def normalizar_colunas(df):
    """Padroniza nomes de colunas e remove duplicatas."""
    novas_colunas = []
    for col in df.columns:
        c = col.replace('"', '').strip()
        c = remover_acentos(c).upper()
        novas_colunas.append(c)
    df.columns = novas_colunas
    return df.loc[:, ~df.columns.duplicated()]

def canonizar_nome(texto):
    if pd.isna(texto): return ""
    t = remover_acentos(str(texto)).upper()
    return re.sub(r'[^A-Z0-9]', '', t)

def limpar_esfera(texto):
    if pd.isna(texto): return "NAO IDENTIFICADO"
    t = remover_acentos(str(texto)).upper()
    if 'FED' in t: return 'FEDERAL'
    if 'EST' in t: return 'ESTADUAL'
    if 'MUN' in t: return 'MUNICIPAL'
    return "OUTROS"

def limpar_status_produto(texto):
    if pd.isna(texto): return "NAO INFORMADO"
    t = str(texto).upper()
    if "REPROVADO" in t: return "REPROVADO"
    if "APROVADO" in t: return "APROVADO"
    if "ANALISE" in t: return "EM ANALISE"
    if "CORRECAO" in t: return "EM CORRECAO"
    if "REALIZADO" in t: return "REALIZADO"
    return "OUTROS"

def achar_coluna(df, termos):
    for col in df.columns:
        for termo in termos:
            if termo in col: return col
    return None

def separar_codigo_produto(texto):
    if pd.isna(texto): return "ND", "Não Informado"
    texto = str(texto).strip()
    partes = texto.split(' - ', 1)
    if len(partes) > 1: return partes[0].strip(), partes[1].strip()
    else: return texto.split(' ')[0][:15], texto

# ==============================================================================
# 1. PROCESSAMENTO DE ACIDENTES PRF
# ==============================================================================
def processar_acidentes_prf(PLANILHAS):
    arquivos = [f for f in os.listdir(PLANILHAS) if f.startswith('acidentes') and f.endswith('.csv')]
    lista_dfs = []
    print("\n--- PROCESSANDO DADOS PRF ---")
    
    for arq in arquivos:
        caminho = os.path.join(PLANILHAS, arq)
        try:
            try: df = pd.read_csv(caminho, encoding='utf-8', sep=';', low_memory=False, on_bad_lines='skip')
            except: df = pd.read_csv(caminho, encoding='latin-1', sep=';', low_memory=False, on_bad_lines='skip')
            
            df = normalizar_colunas(df)
            
            try:
                ano = int(re.search(r'202\d', arq).group())
                df['ANO'] = ano
            except:
                if 'DATA_INVERSA' in df.columns:
                    df['ANO'] = pd.to_datetime(df['DATA_INVERSA'], errors='coerce').dt.year.fillna(0).astype(int)

            if 'DATA_INVERSA' in df.columns:
                df['DATA_INVERSA'] = pd.to_datetime(df['DATA_INVERSA'], errors='coerce')
                df['MES'] = df['DATA_INVERSA'].dt.month.fillna(0).astype(int)
            else:
                df['MES'] = 0

            cols_texto = ['MARCA', 'TIPO_VEICULO', 'SEXO', 'ESTADO_FISICO', 'CAUSA_PRINCIPAL', 
                          'TIPO_ACIDENTE', 'MUNICIPIO', 'UF', 'BR', 'TRACADO_VIA', 'REGIONAL', 
                          'DELEGACIA', 'UOP', 'CONDICAO_METEREOLOGICA', 'SENTIDO_VIA', 'TIPO_PISTA', 
                          'USO_SOLO', 'TIPO_ENVOLVIDO', 'CLASSIFICACAO_ACIDENTE', 'FASE_DIA']
            
            for col in cols_texto:
                if col not in df.columns: df[col] = 'NÃO INFORMADO'
                df[col] = df[col].astype(str).fillna('NÃO INFORMADO').replace('nan', 'NÃO INFORMADO')

            if 'CAUSA_ACIDENTE' in df.columns and 'CAUSA_PRINCIPAL' in df.columns:
                if df['CAUSA_PRINCIPAL'].iloc[0] in ['Sim', 'Não', 'True', 'False']:
                    df['CAUSA_PRINCIPAL'] = df['CAUSA_ACIDENTE']

            cols_num = ['IDADE', 'ILESOS', 'FERIDOS_LEVES', 'FERIDOS_GRAVES', 'MORTOS', 'FERIDOS', 'ID', 'PESID', 'ID_VEICULO', 'ANO_FABRICACAO_VEICULO']
            for col in cols_num:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                else:
                    df[col] = 0
            
            if df['FERIDOS'].sum() == 0:
                df['FERIDOS'] = df['FERIDOS_LEVES'] + df['FERIDOS_GRAVES']

            for c in ['LATITUDE', 'LONGITUDE']:
                if c in df.columns:
                    df[c] = df[c].astype(str).str.replace(',', '.').apply(lambda x: pd.to_numeric(x, errors='coerce'))

            df = df.loc[:, ~df.columns.duplicated()]
            print(f"  ✓ {arq}: {len(df):,} linhas processadas.")
            lista_dfs.append(df)
        except Exception as e:
            print(f"  ERRO ao processar {arq}: {e}")

    if lista_dfs: return pd.concat(lista_dfs, ignore_index=True)
    return pd.DataFrame()

def salvar_prf_rapido(df):
    if df.empty: return False
    df = df.loc[:, ~df.columns.duplicated()]
    
    print(f"\n--- SALVANDO DADOS NO BANCO ({len(df):,} linhas) ---")
    try:
        with engine_principal.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS acidentes_prf"))
            conn.commit()
        
        tipos_colunas = {
            'ID': Integer(), 'PESID': Integer(), 'DATA_INVERSA': Date(),
            'DIA_SEMANA': Text(), 'HORARIO': String(50),
            'UF': String(10), 'BR': Text(),
            'KM': String(50), 'MUNICIPIO': Text(),
            'CAUSA_PRINCIPAL': Text(), 'TIPO_ACIDENTE': Text(),
            'CLASSIFICACAO_ACIDENTE': Text(), 'FASE_DIA': Text(),
            'SENTIDO_VIA': Text(), 'CONDICAO_METEREOLOGICA': Text(),
            'TIPO_PISTA': Text(), 'TRACADO_VIA': Text(),
            'USO_SOLO': Text(), 'ID_VEICULO': Integer(),
            'TIPO_VEICULO': Text(), 'MARCA': Text(),
            'ANO_FABRICACAO_VEICULO': Integer(), 'TIPO_ENVOLVIDO': Text(),
            'ESTADO_FISICO': Text(), 'IDADE': Integer(), 'SEXO': Text(),
            'ILESOS': Integer(), 'FERIDOS_LEVES': Integer(), 'FERIDOS_GRAVES': Integer(),
            'MORTOS': Integer(), 'LATITUDE': Float(), 'LONGITUDE': Float(),
            'REGIONAL': Text(), 'DELEGACIA': Text(), 'UOP': Text(),
            'ANO': Integer(), 'MES': Integer(), 'FERIDOS': Integer()
        }
        
        colunas_validas = [c for c in df.columns if c in tipos_colunas.keys()]
        df_final = df[colunas_validas]
        
        df_final.head(0).to_sql('acidentes_prf', con=engine_principal, if_exists='replace', index=False, dtype=tipos_colunas)
        
        num_workers = max(1, os.cpu_count() - 1)
        tamanho_chunk = math.ceil(len(df_final) / num_workers)
        chunks = [df_final[i:i + tamanho_chunk] for i in range(0, len(df_final), tamanho_chunk)]
        
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            list(executor.map(worker_salvar_chunk, chunks))
            
        print("  -> Criando índices...")
        with engine_principal.connect() as conn:
            conn.execute(text("CREATE INDEX idx_ano ON acidentes_prf (ANO)"))
            conn.execute(text("CREATE INDEX idx_uf ON acidentes_prf (UF)"))
            conn.commit()
            
        print("  ✓ SUCESSO! Dados PRF salvos.")
        return True
    except Exception as e:
        print(f"  ERRO CRÍTICO NO SALVAMENTO: {e}")
        return False

# ==============================================================================
# 2. PROCESSAMENTO DE GESTÃO (COMPLETO E ATUALIZADO COM DATA DE CADASTRO)
# ==============================================================================
def processar_gestao(PLANILHAS):
    print("\n--- PROCESSANDO DADOS DE GESTÃO ---")
    dfs = {}
    arquivos = {'prod':'Produtos.csv', 'org':'Orgaos.csv', 'novos':'NovosProdutos.csv', 'users':'Usuarios.csv'}
    
    for k, nome in arquivos.items():
        try:
            path = os.path.join(PLANILHAS, nome)
            if os.path.exists(path):
                try: df = pd.read_csv(path, dtype=str, encoding='utf-8', sep=None, engine='python')
                except: df = pd.read_csv(path, dtype=str, encoding='latin-1', sep=None, engine='python')
                dfs[k] = normalizar_colunas(df)
                print(f"  ✓ {nome} carregado.")
        except: pass

    df_res, df_org, df_novos = dfs.get('prod'), dfs.get('org'), dfs.get('novos')
    df_ranking, df_stats_status, df_top_produtos, df_top_mun, df_full = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Produtos
    if df_res is not None:
        lista_status = []
        col_uf = achar_coluna(df_res, ['UF', 'ESTADO'])
        col_status = achar_coluna(df_res, ['STATUS'])
        col_prod = achar_coluna(df_res, ['PRODUTO', 'META'])
        col_mun = achar_coluna(df_res, ['MUNICIPIO', 'CIDADE'])
        
        # PROCURA A COLUNA DE DATA DE CADASTRO
        # Você pode adicionar o nome exato da sua coluna aqui nesta lista caso seja um nome diferente
        col_data = achar_coluna(df_res, ['DATA CADASTRO', 'DATA_CADASTRO', 'CRIADO EM', 'DATA', 'DATA DE CADASTRO'])
        
        if col_uf:
            temp = df_res.copy()
            temp['UF_LIMPA'] = temp[col_uf].str.upper().str.strip()
            temp['STATUS_LIMPO'] = temp[col_status].apply(limpar_status_produto) if col_status else "REALIZADO"
            temp['MUNICIPIO_LIMPO'] = temp[col_mun].str.upper().str.strip() if col_mun else "NAO INFORMADO"
            
            # EXTRAI A DATA E ADICIONA À TABELA
            temp['DATA_CADASTRO'] = temp[col_data] if col_data else None
            
            prod_full = temp[col_prod].str.strip() if col_prod else "NAO INFORMADO"
            codigos_desc = prod_full.apply(separar_codigo_produto)
            temp['COD_PRODUTO'] = [x[0] for x in codigos_desc]
            temp['DESC_PRODUTO'] = [x[1] for x in codigos_desc]
            
            # ADICIONA A COLUNA NO RESULTADO FINAL
            lista_status.append(temp[['UF_LIMPA', 'STATUS_LIMPO', 'MUNICIPIO_LIMPO', 'COD_PRODUTO', 'DESC_PRODUTO', 'DATA_CADASTRO']])

        if df_novos is not None:
            c_org = achar_coluna(df_novos, ['ENTIDADE', 'ORGAO'])
            c_st = achar_coluna(df_novos, ['STATUS'])
            c_data_novos = achar_coluna(df_novos, ['DATA CADASTRO', 'DATA_CADASTRO', 'CRIADO EM', 'DATA', 'DATA DE CADASTRO'])
            
            if c_org and c_st:
                t2 = df_novos.copy()
                t2['UF_LIMPA'] = t2[c_org].astype(str).str.extract(r'/([A-Z]{2})')
                t2['STATUS_LIMPO'] = t2[c_st].apply(limpar_status_produto)
                t2['MUNICIPIO_LIMPO'] = "NAO INFORMADO"
                t2['COD_PRODUTO'] = "NOVO"
                t2['DESC_PRODUTO'] = "Novo Produto Cadastrado"
                
                # DATA DE CADASTRO DOS PRODUTOS NOVOS
                t2['DATA_CADASTRO'] = t2[c_data_novos] if c_data_novos else None
                
                lista_status.append(t2.dropna(subset=['UF_LIMPA'])[['UF_LIMPA', 'STATUS_LIMPO', 'MUNICIPIO_LIMPO', 'COD_PRODUTO', 'DESC_PRODUTO', 'DATA_CADASTRO']])

        if lista_status:
            df_full = pd.concat(lista_status)
            df_full = df_full.loc[:, ~df_full.columns.duplicated()]
            
            df_stats_status = df_full.groupby(['UF_LIMPA', 'STATUS_LIMPO']).size().reset_index(name='Quantidade')
            df_ranking = df_stats_status.groupby('UF_LIMPA')['Quantidade'].sum().reset_index(name='Total')
            ufs_base = pd.DataFrame({'UF': ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']})
            df_ranking = pd.merge(ufs_base, df_ranking, left_on='UF', right_on='UF_LIMPA', how='left').fillna(0)
            if 'UF_LIMPA' in df_ranking.columns: del df_ranking['UF_LIMPA']

            df_top_produtos = df_full.groupby(['COD_PRODUTO', 'DESC_PRODUTO']).size().reset_index(name='Quantidade')
            df_top_mun = df_full[df_full['MUNICIPIO_LIMPO'] != 'NAO INFORMADO']['MUNICIPIO_LIMPO'].value_counts().head(50).reset_index()
            df_top_mun.columns = ['Municipio', 'Quantidade']

    # Órgãos
    if df_org is not None and df_res is not None:
        c_org = achar_coluna(df_org, ['NOME', 'ORGAO'])
        c_ent = achar_coluna(df_res, ['ENTIDADE', 'ORGAO'])
        if c_org and c_ent:
            df_org['CHAVE'] = df_org[c_org].apply(canonizar_nome)
            enviaram = set(df_res[c_ent].apply(canonizar_nome).unique())
            if df_novos is not None:
                c_n = achar_coluna(df_novos, ['ENTIDADE', 'ORGAO'])
                if c_n: enviaram.update(df_novos[c_n].apply(canonizar_nome).unique())
            df_org['ENVIOU_PRODUTO'] = df_org['CHAVE'].apply(lambda x: 'SIM' if x in enviaram and x != "" else 'NAO')
            c_esf = achar_coluna(df_org, ['ESFERA'])
            df_org['ESFERA_LIMPA'] = df_org[c_esf].apply(limpar_esfera) if c_esf else "NAO IDENTIFICADO"

    # Salvando Gestão no BD
    salvar_tabela_segura(dfs.get('users'), 'usuarios')
    if df_org is not None:
        cols = [c for c in df_org.columns if c != 'CHAVE']
        salvar_tabela_segura(df_org[cols], 'orgaos_completo')
        
    salvar_tabela_segura(df_ranking, 'ranking_uf')
    salvar_tabela_segura(df_stats_status, 'stats_status_uf')
    salvar_tabela_segura(df_top_produtos, 'stats_produtos')
    salvar_tabela_segura(df_top_mun, 'stats_municipios')
    
    # NOVO: Salva a tabela completa com as Datas para uso na Análise Temporal!
    if not df_full.empty:
        salvar_tabela_segura(df_full, 'produtos_completo')
    
    print("  ✓ Dados de Gestão salvos.")

# ==============================================================================
# MAIN
# ==============================================================================
def processar_tudo():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    PLANILHAS = os.path.join(os.path.dirname(BASE_DIR), 'Planilhas')
    
    # 1. Roda a Gestão
    processar_gestao(PLANILHAS)
    
    # 2. Roda a PRF
    df_prf = processar_acidentes_prf(PLANILHAS)
    salvar_prf_rapido(df_prf)
    
    print("\nETL FINALIZADO COM SUCESSO!")

if __name__ == "__main__":
    processar_tudo()