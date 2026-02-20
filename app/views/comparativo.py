import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from utils import padronizar_grafico

# Dicionário de normalização para garantir que nomes de estados virem siglas padrão
MAPA_ESTADOS = {
    'ACRE': 'AC', 'ALAGOAS': 'AL', 'AMAPA': 'AP', 'AMAZONAS': 'AM',
    'BAHIA': 'BA', 'CEARA': 'CE', 'DISTRITO FEDERAL': 'DF',
    'ESPIRITO SANTO': 'ES', 'GOIAS': 'GO', 'MARANHAO': 'MA', 
    'MATO GROSSO': 'MT', 'MATO GROSSO DO SUL': 'MS',
    'MINAS GERAIS': 'MG', 'PARA': 'PA', 'PARAIBA': 'PB',
    'PARANA': 'PR', 'PERNAMBUCO': 'PE', 'PIAUI': 'PI',
    'RIO DE JANEIRO': 'RJ', 'RIO GRANDE DO NORTE': 'RN', 'RIO GRANDE DO SUL': 'RS',
    'RONDONIA': 'RO', 'RORAIMA': 'RR', 'SANTA CATARINA': 'SC',
    'SAO PAULO': 'SP', 'SERGIPE': 'SE', 'TOCANTINS': 'TO'
}

def normalizar_texto(texto):
    """Padronização para comparação segura entre strings (Mogi == MOJI)."""
    if pd.isna(texto): return ""
    import unicodedata
    s = str(texto).upper().strip()
    nfkd = unicodedata.normalize('NFKD', s)
    return "".join([c for c in nfkd if not unicodedata.combining(c)])

def render_comparativo(df_prod_raw, df_prf_raw, tema):
    st.markdown("### ⚖️ Comparativo PNATRANS vs. Vítimas Fatais (PRF)")
    st.info("Este painel cruza o volume de metas entregues com o total de mortos registrados pela PRF em rodovias federais.")

    if df_prod_raw.empty or df_prf_raw.empty:
        st.warning("⚠️ Dados insuficientes no banco para gerar a comparação.")
        return

    # --- 1. PREPARAÇÃO DOS DATASETS (Conforme memorizado do render_prf) ---
    # Produtos PNATRANS
    df_p = df_prod_raw.copy()
    df_p.columns = [c.upper() for c in df_p.columns]
    if 'STATUS_LIMPO' in df_p.columns:
        df_p = df_p[df_p['STATUS_LIMPO'] == 'REALIZADO']

    # Dados PRF
    df_prf = df_prf_raw.copy()
    df_prf.columns = [c.upper() for c in df_prf.columns]
    if 'MORTOS' not in df_prf.columns:
        st.warning("⚠️ O conjunto de dados PRF não contém a coluna 'MORTOS'. Não é possível gerar o comparativo.")
        return
    df_prf['MORTOS'] = pd.to_numeric(df_prf['MORTOS'], errors='coerce').fillna(0)

    # --- 2. SIDEBAR: FILTROS INTEGRADOS ---
    st.sidebar.divider()
    st.sidebar.subheader("🎯 Filtros de Cruzamento")
    
    col_uf_p = 'UF_LIMPA' if 'UF_LIMPA' in df_p.columns else 'UF'
    col_mun_p = 'MUNICIPIO_LIMPO' if 'MUNICIPIO_LIMPO' in df_p.columns else 'MUNICIPIO'
    
    # Lista de UFs baseada nos dados de gestão
    lista_ufs = sorted(df_p[col_uf_p].dropna().unique())
    sel_uf = st.sidebar.selectbox("🗺️ Selecione a UF:", ["BRASIL (Todas as BRs)"] + lista_ufs)

    sel_mun = "Todos os Municípios"
    if sel_uf != "BRASIL (Todas as BRs)":
        muns_disponiveis = sorted(df_p[df_p[col_uf_p] == sel_uf][col_mun_p].dropna().unique())
        sel_mun = st.sidebar.selectbox("🏙️ Selecione o Município:", ["Todos os Municípios"] + muns_disponiveis)

    # --- 3. APLICAÇÃO DA LÓGICA DE FILTRAGEM ---
    titulo_local = "Brasil (Visão Consolidada)"
    
    if sel_uf != "BRASIL (Todas as BRs)":
        df_p = df_p[df_p[col_uf_p] == sel_uf]
        df_prf = df_prf[df_prf['UF'] == sel_uf]
        titulo_local = f"Rodovias Federais em {sel_uf}"
        
        if sel_mun != "Todos os Municípios":
            df_p = df_p[df_p[col_mun_p] == sel_mun]
            # Normalização de texto para garantir o match (PRF costuma vir sem acentos)
            df_prf['_MUN_NORM'] = df_prf['MUNICIPIO'].apply(normalizar_texto)
            df_prf = df_prf[df_prf['_MUN_NORM'] == normalizar_texto(sel_mun)]
            titulo_local = f"Município: {sel_mun} / {sel_uf}"

    # --- 4. PROCESSAMENTO TEMPORAL (AGRUPAMENTO POR ANO) ---
    # Produtos: Extração de ano da coluna de data
    col_data_p = 'DATA_CADASTRO' if 'DATA_CADASTRO' in df_p.columns else 'DATA'
    df_p['ANO_REF'] = pd.to_datetime(df_p[col_data_p], errors='coerce', dayfirst=True).dt.year
    df_p_ano = df_p.dropna(subset=['ANO_REF']).groupby('ANO_REF').size().reset_index(name='METAS')
    df_p_ano.rename(columns={'ANO_REF': 'ANO'}, inplace=True)

    # PRF: Agrupamento da soma de mortos por ano
    # (Se o arquivo PRF já tiver a coluna ANO, usamos ela; se não, extraímos da DATA_INVERSA)
    if 'ANO' not in df_prf.columns and 'DATA_INVERSA' in df_prf.columns:
        df_prf['ANO'] = pd.to_datetime(df_prf['DATA_INVERSA'], errors='coerce').dt.year
    
    df_prf_ano = df_prf.groupby('ANO')['MORTOS'].sum().reset_index(name='OBITOS_PRF')
    
    # --- 5. UNIÃO E CÁLCULOS ---
    df_comp = pd.merge(df_p_ano, df_prf_ano, on='ANO', how='outer').fillna(0).sort_values('ANO')
    df_comp = df_comp[(df_comp['ANO'] >= 2018) & (df_comp['ANO'] <= 2026)]

    if df_comp.empty or (df_comp['METAS'].sum() == 0 and df_comp['OBITOS_PRF'].sum() == 0):
        st.info(f"Sem dados suficientes para exibir a correlação em {titulo_local}.")
        return

    # Correlação de Pearson
    correlacao = df_comp['METAS'].corr(df_comp['OBITOS_PRF']) if len(df_comp) > 1 else 0

    # KPIs superiores
    c1, c2, c3 = st.columns(3)
    c1.metric("Metas PNATRANS", f"{df_comp['METAS'].sum():.0f}", f"{titulo_local}")
    c2.metric("Mortes Registradas (PRF)", f"{df_comp['OBITOS_PRF'].sum():.0f}", delta_color="inverse")
    
    status_msg = "✅ Impacto Positivo (Mortes em queda)" if correlacao < -0.3 else "⏳ Avaliando Impacto"
    c3.metric("Correlação Entregas/Óbitos", f"{correlacao:.2f}", status_msg)

    # --- 6. GRÁFICO DUAL AXIS ---
    # [Image of dual axis chart comparing road safety goals and traffic fatalities]
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Barras: Metas Realizadas (Produtos)
    fig.add_trace(go.Bar(
        x=df_comp['ANO'], y=df_comp['METAS'], name="Metas Realizadas",
        marker_color='#00CC96', opacity=0.7, text=df_comp['METAS'], textposition='outside'
    ), secondary_y=False)

    # Linha: Óbitos PRF
    fig.add_trace(go.Scatter(
        x=df_comp['ANO'], y=df_comp['OBITOS_PRF'], name="Vítimas Fatais (PRF)",
        mode='lines+markers+text', text=df_comp['OBITOS_PRF'].astype(int),
        textposition='bottom center', line=dict(color='#EF553B', width=4),
        marker=dict(size=10, symbol='diamond')
    ), secondary_y=True)

    fig.update_layout(
        title=f"<b>Análise de Eficácia:</b> Entregas vs. Sinistralidade PRF - {titulo_local}",
        legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
        height=550, margin=dict(t=100)
    )
    
    fig.update_xaxes(type='category', title_text="Série Histórica (Ano)")
    fig.update_yaxes(title_text="Quantidade de Produtos", secondary_y=False, showgrid=False)
    fig.update_yaxes(title_text="Total de Óbitos (Vítimas Fatais)", secondary_y=True, showgrid=True)

    st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

    with st.expander("📊 Tabela Detalhada do Cruzamento"):
        st.dataframe(df_comp.set_index('ANO'), use_container_width=True)