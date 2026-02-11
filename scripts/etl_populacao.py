import pandas as pd
from sqlalchemy import create_engine, text
import re
import os

# --- CONFIGURAÇÕES ---
# Caminho do arquivo (Ajuste se necessário)
ARQUIVO_ODS = 'Planilhas/Municipios.ods' 

# CONEXÃO COM O MYSQL (A mesma usada no seu settings.py/etl_process.py)
# Usuário: root, Senha: Jjjb3509, Banco: db_pnatrans
STRING_CONEXAO = 'mysql+pymysql://root:Jjjb3509@127.0.0.1:3306/db_pnatrans'

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
    print(f"💾 Salvando tabela '{nome_tabela}' no banco MySQL...")
    try:
        # Usamos chunksize para não sobrecarregar o envio de dados
        df.to_sql(nome_tabela, con=engine, if_exists='replace', index=False, chunksize=1000)
        
        # Cria índices para o Dashboard ficar rápido
        with engine.connect() as conn:
            # Índices para Municípios
            if nome_tabela == 'populacao_ibge':
                conn.execute(text("CREATE INDEX idx_pop_uf ON populacao_ibge (uf)"))
                conn.execute(text("CREATE INDEX idx_pop_mun ON populacao_ibge (municipio)"))
                conn.commit()
                
        print(f"✅ Tabela '{nome_tabela}' atualizada com sucesso ({len(df)} registros).")
    except Exception as e:
        print(f"❌ Erro ao salvar '{nome_tabela}': {e}")

def processar_planilha():
    if not os.path.exists(ARQUIVO_ODS):
        print(f"❌ ERRO: O arquivo '{ARQUIVO_ODS}' não foi encontrado.")
        return

    try:
        engine = create_engine(STRING_CONEXAO)
        print(f"🔌 Conectado ao banco MySQL.")
    except Exception as e:
        print(f"❌ ERRO ao conectar no banco: {e}")
        return

    print(f"📂 Lendo planilha: {ARQUIVO_ODS} ...")
    
    try:
        # Requer: pip install odfpy
        dict_abas = pd.read_excel(ARQUIVO_ODS, engine='odf', sheet_name=None, header=1)
    except Exception as e:
        print(f"❌ Erro ao ler o arquivo ODS. Verifique se instalou o odfpy (pip install odfpy): {e}")
        return

    # --- PROCESSAMENTO ---
    dfs_para_salvar = []

    for nome_aba, df_original in dict_abas.items():
        # Normaliza colunas
        cols = [str(c).upper().strip() for c in df_original.columns]
        df_original.columns = cols
        
        # Identifica abas úteis
        tem_municipio = any('MUNIC' in c for c in cols)
        tem_populacao = any('POPULAÇ' in c for c in cols)

        # Processa apenas abas que parecem ter dados de municípios
        if tem_municipio and tem_populacao:
            print(f"📍 Processando aba: '{nome_aba}'")
            
            mapa = {
                'UF': 'uf', 
                'NOME DO MUNIC': 'municipio', 
                'POPULAÇ': 'populacao'
            }
            
            # Filtra e renomeia colunas
            colunas_existentes = {k: v for k, v in mapa.items() if any(k in c for c in df_original.columns)}
            
            # Encontra o nome exato da coluna na planilha
            mapa_real = {}
            for chave_busca, nome_final in colunas_existentes.items():
                col_real = next((c for c in df_original.columns if chave_busca in c), None)
                if col_real: mapa_real[col_real] = nome_final

            df = df_original.rename(columns=mapa_real)
            
            # Mantém apenas as colunas mapeadas
            colunas_finais = list(mapa_real.values())
            df = df[colunas_finais].copy()
            
            # Limpezas
            df = df.dropna(subset=['municipio'])
            df['populacao'] = df['populacao'].apply(limpar_populacao)
            
            # Remove totalizadores (ex: "Brasil", "Norte") se estiverem na coluna município
            termos_ignorar = ['BRASIL', 'REGIÃO', 'UNIDADE DA FEDERAÇÃO']
            df = df[~df['municipio'].str.upper().isin(termos_ignorar)]

            dfs_para_salvar.append(df)

    if dfs_para_salvar:
        # Junta todas as abas (caso os municípios estejam separados por região nas abas)
        df_final = pd.concat(dfs_para_salvar, ignore_index=True)
        
        # Salva na tabela 'populacao_ibge' que o Dashboard vai usar
        salvar_no_banco(df_final, 'populacao_ibge', engine)
    else:
        print("⚠️ Nenhuma aba com dados de município/população encontrada.")

    print("\n--- 🏁 Processo Concluído! ---")

if __name__ == "__main__":
    processar_planilha()