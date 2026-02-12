import streamlit as st
import pandas as pd
import json
import urllib.request
from sqlalchemy import create_engine

# Tenta importar configurações, com fallback seguro para credenciais locais
try:
    from config.settings import DATABASE_URL
except ImportError:
    # Ajuste suas credenciais locais aqui se o arquivo settings.py não for encontrado
    DATABASE_URL = 'mysql+pymysql://root:root@127.0.0.1:3306/db_pnatrans'

# --- 1. CONFIGURAÇÃO DE TEMA ---
def get_tema_config(tema_selecionado):
    if tema_selecionado == "Escuro":
        return {
            "bg_card": "#1E1E1E", "text_color": "#FFFFFF", "sub_text_color": "#AAAAAA",
            "border_color": "#333333", "chart_template": "plotly_dark", "chart_font_color": "white",
            "bg_app": "#0E1117", "sidebar_bg": "#262730", "grid_color": "#444444", "border_width": "1px"
        }
    return {
        "bg_card": "#FFFFFF", "text_color": "#333333", "sub_text_color": "#555555",
        "border_color": "#2196F3", "chart_template": "plotly_white", "chart_font_color": "#333333",
        "bg_app": "#F5F7FA", "sidebar_bg": "#1E2A38", "grid_color": "#EEEEEE", "border_width": "2px"
    }

# --- 2. FUNÇÕES AUXILIARES (HELPERS) ---
def padronizar_grafico(fig, tema):
    """Aplica o tema selecionado aos gráficos Plotly."""
    fig.update_layout(
        template=tema['chart_template'],
        font=dict(color=tema['chart_font_color']),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=30, b=20),
    )
    return fig

def html_card(titulo, valor, subtitulo, tema):
    """Gera o HTML para os cartões de KPI."""
    return f"""
    <div class="card">
        <div class="card-title">{titulo}</div>
        <div class="card-value" style="color:{tema['text_color']}!important;">{valor}</div>
        <div class="card-sub">{subtitulo}</div>
    </div>
    """

def converter_csv(df):
    """Converte DataFrame para CSV (para download)."""
    return df.to_csv(index=False).encode('utf-8')

def limpar_coordenadas(valor):
    """Trata strings de coordenadas substituindo vírgula por ponto."""
    try:
        if pd.isna(valor) or valor == '': return None
        return float(str(valor).replace(',', '.'))
    except: return None

@st.cache_data(ttl=3600)
def carregar_geojson():
    """Baixa o GeoJSON dos estados do Brasil para os mapas."""
    try:
        url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
        with urllib.request.urlopen(url) as response:
            return json.load(response)
    except Exception as e:
        st.error(f"Erro ao carregar mapa (GeoJSON): {e}")
        return None

# --- 3. FUNÇÕES DE CARREGAMENTO DE DADOS (APENAS BANCO DE DADOS) ---

@st.cache_data(ttl=300)
def carregar_dados_gerais():
    """
    Carrega as tabelas principais para o Painel Geral.
    Retorna 7 DataFrames.
    """
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            # Tabelas essenciais
            df_mapa = pd.read_sql("SELECT * FROM ranking_uf", conn)
            df_org = pd.read_sql("SELECT * FROM orgaos_completo", conn)
            df_prod = pd.read_sql("SELECT * FROM stats_produtos", conn)
            
            # Tabelas com tratamento de erro (caso ainda não existam no banco)
            try: df_status = pd.read_sql("SELECT * FROM stats_status_uf", conn)
            except: df_status = pd.DataFrame()
            
            try: df_users = pd.read_sql("SELECT * FROM usuarios", conn)
            except: df_users = pd.DataFrame()
            
            try: df_mun = pd.read_sql("SELECT * FROM stats_municipios", conn)
            except: df_mun = pd.DataFrame()

            # Tenta carregar dados brutos de produtos
            try: df_raw = pd.read_sql("SELECT * FROM produtos_raw", conn)
            except: df_raw = pd.DataFrame()
            
            return df_mapa, df_org.fillna("-"), df_prod.fillna(0), df_status.fillna(0), df_users.fillna("-"), df_raw, df_mun
            
    except Exception as e:
        st.error(f"Erro Crítico ao carregar dados gerais: {e}")
        # Retorna tupla com 7 dataframes vazios para não quebrar a UI
        return (pd.DataFrame(),) * 7

@st.cache_data(ttl=300)
def carregar_dados_prf():
    """Carrega dados da tabela 'acidentes_prf'."""
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            df = pd.read_sql("SELECT * FROM acidentes_prf", conn)
            
            # Tratamento de coordenadas se existirem
            if not df.empty and 'LATITUDE' in df.columns:
                df['LAT'] = df['LATITUDE'].apply(limpar_coordenadas)
                df['LON'] = df['LONGITUDE'].apply(limpar_coordenadas)
            
            return df.fillna("NÃO INFORMADO")
    except Exception as e:
        # st.error(f"Erro PRF: {e}") # Opcional: Descomente para debug
        return pd.DataFrame()

@st.cache_data(ttl=300)
def carregar_dados_obitos():
    """Carrega dados da tabela 'obitos_transporte'."""
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            return pd.read_sql("SELECT * FROM obitos_transporte", conn)
    except Exception as e:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def carregar_dados_comparativo():
    """
    Carrega as tabelas necessárias para a aba 'Relação Produtos x Óbitos'.
    Tenta carregar 'produtos_resultados', se falhar, usa 'produtos_raw'.
    """
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            # 1. Carrega Produtos
            try:
                # Tenta a tabela processada primeiro
                df_prod = pd.read_sql("SELECT * FROM produtos_resultados", conn)
            except:
                # Fallback para a tabela bruta
                df_prod = pd.read_sql("SELECT * FROM produtos_raw", conn)
            
            # 2. Carrega Óbitos
            df_obitos = pd.read_sql("SELECT * FROM obitos_transporte", conn)
            
            return df_prod, df_obitos
    except Exception as e:
        st.error(f"Erro ao carregar dados comparativos: {e}")
        return pd.DataFrame(), pd.DataFrame()