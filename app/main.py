import streamlit as st
import os
import sys
from PIL import Image

# --- 1. CONFIGURAÇÃO DE AMBIENTE E PATH ---
# Garante que o Python encontre a pasta raiz do projeto e a pasta config
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# Importações dos módulos internos
try:
    # Importa as views (telas)
    from views import produtos, prf, obitos, comparativo
    # Importa as funções de carregamento (Backend SQL)
    from utils import (
        carregar_dados_gerais, 
        carregar_dados_prf, 
        carregar_dados_obitos, 
        carregar_dados_comparativo, 
        get_tema_config
    )
except ImportError as e:
    st.error(f"Erro de Importação: {e}. Verifique se a estrutura de pastas está correta.")
    st.stop()

# --- 2. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Gestão PNATRANS", 
    layout="wide", 
    page_icon="🚦"
)

# --- 3. BARRA LATERAL (MENU E CONFIGURAÇÕES) ---
st.sidebar.header("⚙️ Configurações")

# Controle de Tema (Persistência via Session State)
if "tema_escuro" not in st.session_state:
    st.session_state.tema_escuro = False

col_toggle, col_txt = st.sidebar.columns([1, 4])
with col_toggle:
    # O toggle atualiza o estado da sessão
    usar_tema_escuro = st.toggle("", value=st.session_state.tema_escuro)
    st.session_state.tema_escuro = usar_tema_escuro

tema_selecionado = "Escuro" if usar_tema_escuro else "Claro"
cfg = get_tema_config(tema_selecionado)

with col_txt:
    st.write(f"Modo **{tema_selecionado}**")

# Botão de Atualização (Limpa o Cache e Recarrega do Banco)
if st.sidebar.button("🔄 Atualizar Dados", help="Recarregar informações do Banco de Dados"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.divider()

# Navegação Principal
st.sidebar.subheader("Navegação")
pagina = st.sidebar.radio(
    "Ir para:", 
    [
        "📊 Painel PNATRANS", 
        "📈 Análise Temporal", 
        "⚖️ Relação Produtos x Óbitos", # <--- NOVA GUIA
        "🚗 Sinistros PRF", 
        "🏥 Óbitos (DATASUS)"
    ],
    label_visibility="collapsed"
)

# --- 4. ESTILIZAÇÃO CSS DINÂMICA ---
st.markdown(f"""
    <style>
        /* Cores Globais */
        .stApp {{ background-color: {cfg['bg_app']}; color: {cfg['text_color']}; }}
        [data-testid="stSidebar"] {{ background-color: {cfg['sidebar_bg']}; }}
        
        /* Texto da Sidebar */
        [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3, 
        [data-testid="stSidebar"] span, [data-testid="stSidebar"] p, [data-testid="stSidebar"] label {{
            color: #FFFFFF !important;
        }}

        /* Botões de Navegação (Estilo Card) */
        [data-testid="stSidebar"] div[role="radiogroup"] > label {{
            background-color: {cfg['bg_card']} !important;
            border: 1px solid {cfg['border_color']} !important;
            padding: 12px 15px !important;
            border-radius: 8px !important;
            margin-bottom: 8px !important;
            transition: transform 0.2s ease, border-color 0.2s ease;
        }}
        [data-testid="stSidebar"] div[role="radiogroup"] > label:hover {{
            border-color: #FFFFFF !important;
            transform: translateX(5px);
        }}
        /* Oculta a bolinha do rádio */
        div[role="radiogroup"] > label > div:first-child {{ display: none !important; }}

        /* Cards de KPI */
        .card {{
            background-color: {cfg['bg_card']}; 
            border: {cfg['border_width']} solid {cfg['border_color']};
            border-radius: 8px; padding: 15px; text-align: center;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .card-title {{ color: {cfg['text_color']}; font-weight: bold; font-size: 0.9rem; text-transform: uppercase; opacity: 0.9; }}
        .card-value {{ color: {cfg['text_color']}; font-weight: 800; font-size: 1.8rem; margin: 5px 0; }}
        .card-sub {{ color: {cfg['sub_text_color']}; font-size: 0.8rem; }}
    </style>
""", unsafe_allow_html=True)

# --- 5. CARREGAMENTO DE DADOS (DATABASE ONLY) ---
with st.spinner("Conectando ao banco de dados..."):
    # Carrega dados gerais iniciais (Cacheado)
    df_mapa, df_org, df_prod, df_status, df_users, df_raw, df_mun = carregar_dados_gerais()

# --- 6. CABEÇALHO DA APLICAÇÃO ---
c_logo, c_titulo = st.columns([1, 8])
img_path = os.path.join(os.path.dirname(__file__), "components", "image.png")

if os.path.exists(img_path):
    with c_logo: st.image(Image.open(img_path), width=110)
else:
    with c_logo: st.write("🚦")

with c_titulo:
    st.title("Painel de Gestão PNATRANS")
    st.markdown("**Monitoramento e Inteligência de Dados Viários**")

st.divider()

# --- 7. ROTEAMENTO DE PÁGINAS ---
if pagina == "📊 Painel PNATRANS":
    produtos.render_visao_geral(df_mapa, df_org, df_prod, df_status, cfg, df_mun, df_users)

elif pagina == "📈 Análise Temporal":
    produtos.render_analise_temporal(df_raw, cfg)

elif pagina == "⚖️ Relação Produtos x Óbitos":
    # Carrega dados específicos para esta tela (Lazy Loading)
    df_p_comp, df_o_comp = carregar_dados_comparativo()
    comparativo.render_comparativo(df_p_comp, df_o_comp, cfg)

elif pagina == "🚗 Sinistros PRF":
    # Carregamento Lazy (sob demanda)
    df_prf = carregar_dados_prf()
    prf.render_prf(df_prf, cfg)

elif pagina == "🏥 Óbitos (DATASUS)":
    # Carregamento Lazy (sob demanda)
    df_obitos = carregar_dados_obitos()
    obitos.render_obitos(df_obitos, cfg)