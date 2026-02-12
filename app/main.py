import streamlit as st
import os
from PIL import Image
# Importa as views
from views import produtos, prf, obitos 
# Importa as funções de carregamento (Versão Original Estável)
from utils import carregar_dados_gerais, carregar_dados_prf, carregar_dados_obitos, get_tema_config

# 1. Configuração da Página
st.set_page_config(
    page_title="Gestão PNATRANS", 
    layout="wide", 
    page_icon="🚦"
)

# 2. Configuração do Menu Lateral
st.sidebar.header("⚙️ Configurações")

# --- Toggle de Tema ---
col_toggle, col_txt = st.sidebar.columns([1, 4])
with col_toggle:
    usar_tema_escuro = st.toggle("", value=False)
tema_selecionado = "Escuro" if usar_tema_escuro else "Claro"
cfg = get_tema_config(tema_selecionado)

with col_txt:
    st.write(f"Modo **{tema_selecionado}**")

# --- Botão Limpar Cache ---
if st.sidebar.button("🔄 Atualizar Dados", help="Recarrega os dados do banco"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.divider()

# --- Navegação ---
st.sidebar.subheader("Navegação")
pagina = st.sidebar.radio(
    "Ir para:", 
    [
        "📊 Painel PNATRANS", 
        "📈 Análise Temporal", 
        "🚗 Sinistros PRF", 
        "🏥 Óbitos (DATASUS)"
    ],
    label_visibility="collapsed"
)

# 3. CSS Otimizado (ESTILOS RESTAURADOS)
st.markdown(f"""
    <style>
        /* Fundo e Texto Global */
        .stApp {{ background-color: {cfg['bg_app']}; color: {cfg['text_color']}; }}
        [data-testid="stSidebar"] {{ background-color: {cfg['sidebar_bg']}; padding-top: 10px; }}
        
        /* Textos da Sidebar brancos */
        [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3, 
        [data-testid="stSidebar"] span, [data-testid="stSidebar"] p, [data-testid="stSidebar"] label {{
            color: #FFFFFF !important;
        }}

        /* --- BOTÕES DE NAVEGAÇÃO (Full Width - Estilo Botão) --- */
        [data-testid="stSidebar"] .stRadio {{ width: 100% !important; }}
        [data-testid="stSidebar"] .stRadio > div {{ width: 100% !important; }}
        [data-testid="stSidebar"] div[role="radiogroup"] {{ width: 100% !important; }}
        
        [data-testid="stSidebar"] div[role="radiogroup"] > label {{
            width: 100% !important;
            display: flex !important;
            justify-content: flex-start !important;
            background-color: {cfg['bg_card']} !important;
            border: 1px solid {cfg['border_color']} !important;
            padding: 12px 15px !important;
            border-radius: 8px !important;
            margin-bottom: 8px !important;
            box-sizing: border-box !important;
            cursor: pointer;
            transition: all 0.2s ease;
        }}

        /* Texto dentro do botão */
        [data-testid="stSidebar"] .stRadio label p {{
            font-size: 1rem !important;
            font-weight: 600 !important;
            color: {cfg['text_color']} !important;
            width: 100%;
        }}

        /* Hover no botão do menu */
        [data-testid="stSidebar"] div[role="radiogroup"] > label:hover {{
            background-color: {cfg['border_color']} !important;
            border-color: #FFFFFF !important;
            transform: translateX(5px);
        }}
        [data-testid="stSidebar"] div[role="radiogroup"] > label:hover p {{
            color: #FFFFFF !important;
        }}

        /* Oculta bolinha radio e títulos plotly */
        div[role="radiogroup"] > label > div:first-child {{ display: none !important; }}
        .gtitle {{ display: none !important; }}
        
        /* Estilo dos Cards KPI */
        .card {{
            background-color: {cfg['bg_card']}; 
            border: {cfg['border_width']} solid {cfg['border_color']};
            border-radius: 8px; padding: 15px; text-align: center;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .card-title {{ color: {cfg['text_color']}; font-weight: bold; opacity: 0.9; font-size: 0.9rem; text-transform: uppercase; }}
        .card-value {{ color: {cfg['text_color']}; font-weight: 800; font-size: 1.8rem; margin: 5px 0; }}
        .card-sub {{ color: {cfg['sub_text_color']}; font-size: 0.8rem; }}
    </style>
""", unsafe_allow_html=True)

# 4. Carregamento Inicial (Gestão)
df_mapa, df_org, df_prod, df_status, df_users, df_raw, df_mun = carregar_dados_gerais()

# 5. Header Principal
c_logo, c_titulo = st.columns([1, 8])
img_path = os.path.join(os.path.dirname(__file__), "components", "image.png")

if os.path.exists(img_path):
    with c_logo: st.image(Image.open(img_path), width=120)
else:
    with c_logo: st.write("🚦")

with c_titulo:
    st.title("Painel do Sistema de Gestão Pnatrans")
    st.markdown("**Inteligência de Dados e Monitoramento Viário**")

st.divider()

# 6. Roteamento de Páginas
if pagina == "📊 Painel PNATRANS":
    produtos.render_visao_geral(df_mapa, df_org, df_prod, df_status, cfg, df_mun, df_users)

elif pagina == "📈 Análise Temporal":
    produtos.render_analise_temporal(df_raw, cfg)

elif pagina == "🚗 Sinistros PRF":
    # Carregamento TOTAL aqui (Método original que garante os dados)
    df_prf = carregar_dados_prf()
    prf.render_prf(df_prf, cfg)

elif pagina == "🏥 Óbitos (DATASUS)":
    df_obitos = carregar_dados_obitos()
    obitos.render_obitos(df_obitos, cfg)