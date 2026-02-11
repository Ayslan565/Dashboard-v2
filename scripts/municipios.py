import pandas as pd
from sqlalchemy import create_engine
import re
import os

# --- CONFIGURAÇÕES ---
# Caminho da planilha (confirme se está correto)
ARQUIVO_ODS = 'Planilhas/Municipios.ods' 

# CONEXÃO COM O BANCO EXISTENTE
# Atenção: Se o seu banco for 'db_pnatrans.db' e estiver na mesma pasta deste script:
STRING_CONEXAO = 'sqlite:///db_pnatrans.db'

# Se o banco estiver dentro da pasta 'app', use:
# STRING_CONEXAO = 'sqlite:///app/db_pnatrans.db'

def limpar_populacao(valor):
    """Remove pontos e notas de rodapé (ex: '12.345(1)' vira 12345)"""
    if pd.isna(valor): return 0
    valor_str = str(valor)
    # Remove conteúdo entre parênteses e pontos
    valor_str = re.sub(r'\s*\(.*\)', '', valor_str).replace('.', '')
    try:
        return int(valor_str)
    except ValueError:
        return 0

def salvar_no_banco(df, nome_tabela, engine):
    print(f"💾 Salvando tabela '{nome_tabela}' no banco 'db_pnatrans'...")
    # 'replace' recria apenas esta tabela específica, mantendo o resto do banco intacto
    df.to_sql(nome_tabela, con=engine, if_exists='replace', index=False)
    print(f"✅ Tabela '{nome_tabela}' atualizada com sucesso ({len(df)} registros).")

def processar_planilha():
    # 1. Verifica arquivo ODS
    if not os.path.exists(ARQUIVO_ODS):
        print(f"❌ ERRO: O arquivo '{ARQUIVO_ODS}' não foi encontrado.")
        return

    # 2. Tenta conectar no banco antes de tudo
    try:
        engine = create_engine(STRING_CONEXAO)
        # Testa conexão
        with engine.connect() as conn:
            pass
        print(f"🔌 Conectado ao banco: {STRING_CONEXAO}")
    except Exception as e:
        print(f"❌ ERRO ao conectar no banco: {e}")
        return

    print(f"📂 Lendo planilha: {ARQUIVO_ODS} ...")
    
    try:
        # Lê TODAS as abas
        dict_abas = pd.read_excel(ARQUIVO_ODS, engine='odf', sheet_name=None, header=1)
    except Exception as e:
        print(f"❌ Erro ao ler o arquivo Excel: {e}")
        return

    # --- PROCESSAMENTO INTELIGENTE DAS ABAS ---
    for nome_aba, df_original in dict_abas.items():
        # Normaliza nomes das colunas
        cols = [c.upper().strip() for c in df_original.columns]
        df_original.columns = cols
        
        # Detecta o conteúdo da aba pelas colunas
        tem_municipio = any('MUNIC' in c for c in cols)
        tem_uf = any('UF' in c or 'UNIDADE DA FEDERAÇÃO' in c for c in cols)
        tem_populacao = any('POPULAÇ' in c for c in cols)

        # --- CASO 1: ABA DE MUNICÍPIOS ---
        if tem_municipio and tem_populacao:
            print(f"\n📍 Processando aba de MUNICÍPIOS: '{nome_aba}'")
            
            mapa = {
                'UF': 'uf', 'COD. UF': 'cod_uf', 'CÓD. MUNIC': 'cod_municipio',
                'NOME DO MUNIC': 'nome_municipio', 'POPULAÇ': 'populacao'
            }
            
            colunas_finais = {}
            for col_planilha in df_original.columns:
                for chave, valor in mapa.items():
                    if chave in col_planilha: colunas_finais[col_planilha] = valor
            
            df = df_original.rename(columns=colunas_finais)
            df = df[list(colunas_finais.values())].copy()
            
            # Limpeza
            df = df.dropna(subset=['nome_municipio'])
            if 'cod_municipio' in df.columns: 
                df = df[df['cod_municipio'] != '00000'] # Remove totalizadores
            
            df['populacao'] = df['populacao'].apply(limpar_populacao)
            
            # Gera ID IBGE Completo (UF + MUNIC)
            if 'cod_uf' in df.columns and 'cod_municipio' in df.columns:
                try:
                    df['cod_uf'] = pd.to_numeric(df['cod_uf'], errors='coerce').fillna(0).astype(int).astype(str)
                    df['cod_municipio'] = pd.to_numeric(df['cod_municipio'], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(5)
                    df['id_ibge'] = df['cod_uf'] + df['cod_municipio']
                except: pass

            salvar_no_banco(df, 'municipios', engine)

        # --- CASO 2: ABA DE ESTADOS ---
        elif tem_uf and tem_populacao and not tem_municipio:
            print(f"\n🗺️ Processando aba de ESTADOS: '{nome_aba}'")
            
            mapa = {
                'UF': 'uf', 'COD. UF': 'cod_uf', 'POPULAÇ': 'populacao',
                'UNIDADE DA FEDERAÇÃO': 'nome_uf'
            }
            
            colunas_finais = {}
            for col_planilha in df_original.columns:
                for chave, valor in mapa.items():
                    if chave in col_planilha: colunas_finais[col_planilha] = valor
            
            df = df_original.rename(columns=colunas_finais)
            df = df[list(colunas_finais.values())].copy()
            
            # Limpeza
            if 'nome_uf' in df.columns:
                df = df[df['nome_uf'].str.upper() != 'BRASIL'] # Remove totalizador Brasil
                df = df.dropna(subset=['nome_uf'])

            df['populacao'] = df['populacao'].apply(limpar_populacao)
            
            if 'cod_uf' in df.columns:
                 df['cod_uf'] = pd.to_numeric(df['cod_uf'], errors='coerce').fillna(0).astype(int)

            salvar_no_banco(df, 'estados', engine)

    print("\n--- 🏁 Processo Concluído! ---")

if __name__ == "__main__":
    processar_planilha()