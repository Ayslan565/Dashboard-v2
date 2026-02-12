import streamlit as st
import pandas as pd
import os
import ssl
import json
from sqlalchemy import create_engine
from urllib.request import urlopen

# --- CONFIGURAÇÃO DE TEMA (CLARO/ESCURO) ---
def get_tema_config(tema_selecionado):
    if tema_selecionado == "Escuro":
        return {
            "bg_card": "#1E1E1E", "text_color": "#FFFFFF", "sub_text_color": "#AAAAAA",
            "border_color": "#333333", "chart_template": "plotly_dark", "chart_font_color": "white",
            "bg_app": "#0E1117", "sidebar_bg": "#262730", "grid_color": "#444444", "border_width": "1px"
        }
    else:
        return {
            "bg_card": "#FFFFFF", "text_color": "#333333", "sub_text_color": "#555555",
            "border_color": "#2196F3", "chart_template": "plotly_white", "chart_font_color": "#333333",
            "bg_app": "#F5F7FA", "sidebar_bg": "#1E2A38", "grid_color": "#EEEEEE", "border_width": "2px"
        }

# --- HELPERS ---
def limpar_coordenadas(valor):
    try:
        if pd.isna(valor) or valor == '': return None
        if isinstance(valor, (int, float)): return float(valor)
        return float(str(valor).replace(',', '.'))
    except: return None

def extrair_hora(valor):
    try:
        val_str = str(valor)
        if ':' in val_str: return int(val_str.split(':')[0])
        return int(float(valor))
    except: return None

def converter_csv(df):
    return df.to_csv(index=False).encode('utf-8')

def html_card(titulo, valor, subtitulo, tema):
    return f"""
    <div class="card">
        <div class="card-title">{titulo}</div>
        <div class="card-value" style="color: {tema['text_color']} !important;">{valor}</div>
        <div class="card-sub">{subtitulo}</div>
    </div>
    """

def padronizar_grafico(fig, tema):
    fig.update_layout(
        template=tema['chart_template'],
        font=dict(color=tema['chart_font_color']),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        title_font_color=tema['chart_font_color'],
        legend_font_color=tema['chart_font_color'],
        margin=dict(l=20, r=20, t=30, b=20),
    )
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor=tema['grid_color'])
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor=tema['grid_color'])
    return fig

# --- CARREGAMENTO GERAL ---
@st.cache_data(ttl=3600)
def carregar_dados_gerais():
    try:
        engine = create_engine('mysql+pymysql://root:Jjjb3509@127.0.0.1:3306/db_pnatrans')
        with engine.connect() as conn:
            df_mapa = pd.read_sql("SELECT * FROM ranking_uf", conn)
            df_org = pd.read_sql("SELECT * FROM orgaos_completo", conn)
            df_prod = pd.read_sql("SELECT * FROM stats_produtos", conn)
            try: df_status = pd.read_sql("SELECT * FROM stats_status_uf", conn)
            except: df_status = pd.DataFrame()
            try: df_users = pd.read_sql("SELECT * FROM usuarios", conn)
            except: df_users = pd.DataFrame()
            try: df_mun = pd.read_sql("SELECT * FROM stats_municipios", conn)
            except: df_mun = pd.DataFrame()

            return df_mapa, df_org.fillna("-"), df_prod.fillna(0), df_status.fillna(0), df_users.fillna("-"), pd.DataFrame(), df_mun
    except Exception as e:
        return (pd.DataFrame(),)*7

# --- CARREGAMENTO PRF ---
@st.cache_data(ttl=3600, show_spinner="Carregando base PRF completa...")
def carregar_dados_prf():
    try:
        engine = create_engine('mysql+pymysql://root:Jjjb3509@127.0.0.1:3306/db_pnatrans')
        with engine.connect() as conn:
            cols = """
                ID, PESID, DATA_INVERSA, DIA_SEMANA, HORARIO, UF, BR, KM, MUNICIPIO,
                CAUSA_PRINCIPAL, TIPO_ACIDENTE, CLASSIFICACAO_ACIDENTE, FASE_DIA,
                SENTIDO_VIA, CONDICAO_METEREOLOGICA, TIPO_PISTA, TRACADO_VIA, USO_SOLO,
                ID_VEICULO, TIPO_VEICULO, MARCA, ANO_FABRICACAO_VEICULO, TIPO_ENVOLVIDO,
                ESTADO_FISICO, IDADE, SEXO,
                ILESOS, FERIDOS_LEVES, FERIDOS_GRAVES, MORTOS, FERIDOS,
                LATITUDE, LONGITUDE, REGIONAL, DELEGACIA, UOP, ANO, MES
            """
            try: df = pd.read_sql(f"SELECT {cols} FROM acidentes_prf", conn)
            except: df = pd.read_sql("SELECT * FROM acidentes_prf", conn)
            
            if not df.empty:
                cols_int = ['ANO', 'MES', 'IDADE', 'ILESOS', 'FERIDOS_LEVES', 'FERIDOS_GRAVES', 'MORTOS', 'FERIDOS', 'ANO_FABRICACAO_VEICULO']
                for c in cols_int:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).astype(int)
                
                if 'LATITUDE' in df.columns: df['LAT'] = df['LATITUDE'].apply(limpar_coordenadas)
                if 'LONGITUDE' in df.columns: df['LON'] = df['LONGITUDE'].apply(limpar_coordenadas)
                if 'HORARIO' in df.columns: df['HORA_INT'] = df['HORARIO'].apply(extrair_hora)
            
            return df.fillna("NÃO INFORMADO")
    except Exception as e:
        return pd.DataFrame()

# --- CARREGAMENTO OBITOS ---
@st.cache_data(ttl=3600, show_spinner="Carregando dados de Óbitos (SIM)...")
def carregar_dados_obitos():
    try:
        engine = create_engine('mysql+pymysql://root:Jjjb3509@127.0.0.1:3306/db_pnatrans')
        with engine.connect() as conn:
            return pd.read_sql("SELECT * FROM obitos_transporte", conn)
    except: return pd.DataFrame()

# --- CARREGAMENTO POPULAÇÃO ---
@st.cache_data(ttl=3600)
def carregar_populacao():
    try:
        engine = create_engine('mysql+pymysql://root:Jjjb3509@127.0.0.1:3306/db_pnatrans')
        with engine.connect() as conn:
            df = pd.read_sql("SELECT uf, municipio, populacao FROM populacao_ibge", conn)
            df['municipio_norm'] = df['municipio'].str.upper().str.strip()
            df['uf_norm'] = df['uf'].str.upper().str.strip()
            return df
    except:
        return pd.DataFrame()

# --- CARREGAMENTO CAPACITAÇÕES (NOVO) ---
@st.cache_data(ttl=300)
def carregar_capacitacoes():
    try:
        engine = create_engine('mysql+pymysql://root:Jjjb3509@127.0.0.1:3306/db_pnatrans')
        with engine.connect() as conn:
            df = pd.read_sql("SELECT * FROM capacitacoes ORDER BY DATA_CAPACITACAO DESC", conn)
            return df
    except:
        return pd.DataFrame()

@st.cache_data
def carregar_geojson():
    url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
    ctx = ssl._create_unverified_context()
    with urlopen(url, context=ctx) as response:
        return json.load(response)