import streamlit as st
import plotly.express as px
import pandas as pd
from utils import html_card, padronizar_grafico, converter_csv, carregar_geojson

def render_visao_geral(df_mapa, df_org, df_prod, df_status, tema, df_mun, df_users=pd.DataFrame()):
    st.markdown(f"### 📊 Visão Geral Completa")
    
    # --- 0. FILTRO DE ESFERA ---
    if not df_org.empty and 'ESFERA_LIMPA' in df_org.columns:
        st.sidebar.divider()
        st.sidebar.markdown("### 🏢 Filtro de Esfera")
        opcoes_esfera = sorted(df_org['ESFERA_LIMPA'].astype(str).unique())
        sel_esfera = st.sidebar.multiselect("Selecione a Esfera:", options=opcoes_esfera, placeholder="Todas as esferas")
        
        if sel_esfera:
            df_org = df_org[df_org['ESFERA_LIMPA'].isin(sel_esfera)]
            if not df_users.empty and 'ORGAO' in df_users.columns and 'NOME' in df_org.columns:
                orgaos_filtrados = df_org['NOME'].unique()
                df_users = df_users[df_users['ORGAO'].isin(orgaos_filtrados)]

    # --- 1. TRATAMENTO DE DADOS DOS USUÁRIOS ---
    if not df_users.empty:
        df_users.columns = [c.upper() for c in df_users.columns]
        if 'PERFIL' in df_users.columns:
            df_users = df_users[df_users['PERFIL'].astype(str).str.contains("PONTO FOCAL", na=False, case=False)]

    # Prepara dados de órgãos ativos (Que enviaram produtos)
    df_ativos = df_org[df_org['ENVIOU_PRODUTO'] == 'SIM'] if not df_org.empty else pd.DataFrame()
    
    # --- LÓGICA DE CLASSIFICAÇÃO DA INICIATIVA PRIVADA ---
    termos_privados = [
        'ABCR', 'ABEETRANS', 'ABNT', 'ABRAPSIT', 'ABSEV', 'AEA', 'ANFAVEA', 'ARTERIS', 
        'LOCADORAS', 'CIDADEAPÉ', 'CNC', 'CNT', 'CNVV', 'FENATRAL', 'FENIVE', 'IBDTRANSITO', 
        'CORDIAL', 'INPROTRAN', 'HONDA', 'YAMAHA', 'NITERÓI TRÂNSITO', 'REDE EDUCATIVA', 
        'SEST SENAT', 'UCB', 'EMPRESA PRIVADA'
    ]
    
    df_privada = pd.DataFrame()
    if not df_ativos.empty:
        mask_termo = df_ativos['NOME'].astype(str).str.upper().apply(lambda x: any(termo in x for termo in termos_privados))
        mask_esfera = df_ativos['ESFERA_LIMPA'].isin(['PRIVADA', 'PARTICULAR', 'INICIATIVA PRIVADA', 'EMPRESA'])
        df_privada = df_ativos[mask_termo | mask_esfera]

    # --- 2. KPIs e Downloads ---
    # AGORA SÃO 7 COLUNAS
    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    
    total_produtos = int(df_mapa['Total'].sum()) if not df_mapa.empty else 0
    total_geral_orgaos = len(df_org) # Total de órgãos cadastrados (Universo)
    total_ativos = len(df_ativos)    # Apenas os que enviaram produtos
    
    # Separa os DataFrames por Esfera para os downloads
    df_federal = df_ativos[df_ativos['ESFERA_LIMPA']=='FEDERAL'] if not df_ativos.empty else pd.DataFrame()
    df_estadual = df_ativos[df_ativos['ESFERA_LIMPA']=='ESTADUAL'] if not df_ativos.empty else pd.DataFrame()
    df_municipal = df_ativos[df_ativos['ESFERA_LIMPA']=='MUNICIPAL'] if not df_ativos.empty else pd.DataFrame()

    # Totais
    total_federais = len(df_federal)
    total_estaduais = len(df_estadual)
    total_municipais = len(df_municipal)
    total_privada = len(df_privada)

    # --- Renderização dos Cards com Botões ---
    
    # Card 1: Produtos
    with c1: 
        st.markdown(html_card("📦 Produtos", total_produtos, "Recebidos", tema), unsafe_allow_html=True)
        if not df_prod.empty: 
            st.download_button("📥 CSV", converter_csv(df_prod), "produtos.csv", key="b1")
            
    # Card 2: TOTAL DE ÓRGÃOS (NOVO)
    with c2:
        st.markdown(html_card("🏢 Total", total_geral_orgaos, "Cadastrados", tema), unsafe_allow_html=True)
        if not df_org.empty:
            st.download_button("📥 CSV", converter_csv(df_org), "lista_completa_orgaos.csv", key="b_total")

    # Card 3: Órgãos Ativos
    with c3: 
        st.markdown(html_card("✅ Ativos", total_ativos, "Enviaram", tema), unsafe_allow_html=True)
        if not df_ativos.empty: 
            st.download_button("📥 CSV", converter_csv(df_ativos), "orgaos_ativos.csv", key="b2")
            
    # Card 4: Federais
    with c4: 
        st.markdown(html_card("🏛️ Federais", total_federais, "Ativos", tema), unsafe_allow_html=True)
        if not df_federal.empty:
            st.download_button("📥 CSV", converter_csv(df_federal), "federais_ativos.csv", key="b3")
            
    # Card 5: Estaduais
    with c5: 
        st.markdown(html_card("🗺️ Estaduais", total_estaduais, "Ativos", tema), unsafe_allow_html=True)
        if not df_estadual.empty:
            st.download_button("📥 CSV", converter_csv(df_estadual), "estaduais_ativos.csv", key="b4")
            
    # Card 6: Municipais
    with c6: 
        st.markdown(html_card("🏙️ Municipais", total_municipais, "Ativos", tema), unsafe_allow_html=True)
        if not df_municipal.empty:
            st.download_button("📥 CSV", converter_csv(df_municipal), "municipais_ativos.csv", key="b5")
            
    # Card 7: Privadas
    with c7:
        st.markdown(html_card("🏭 Privadas", total_privada, "Iniciativas", tema), unsafe_allow_html=True)
        if not df_privada.empty:
            st.download_button("📥 CSV", converter_csv(df_privada), "iniciativa_privada.csv", key="b6")

    st.divider()
    
    # --- 3. Mapa e Status ---
    c_mapa, c_status = st.columns([3, 2])
    with c_mapa:
        st.subheader("🗺️ Mapa de Entregas")
        try:
            if not df_mapa.empty:
                geo = carregar_geojson()
                fig = px.choropleth(df_mapa, geojson=geo, locations='UF', featureidkey="properties.sigla",
                                    color='Total', color_continuous_scale='Reds', scope="south america")
                fig.update_geos(fitbounds="locations", visible=False, bgcolor="rgba(0,0,0,0)")
                fig.update_layout(height=500, margin={"r":0,"t":0,"l":0,"b":0}, dragmode=False)
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True, config={'scrollZoom': False, 'displayModeBar': False})
            else: st.info("Sem dados para o mapa.")
        except: st.warning("Mapa carregando...")

    with c_status:
        st.subheader("🚦 Status por UF")
        if not df_status.empty:
            fig = px.bar(df_status, x="Quantidade", y="UF_LIMPA", color="STATUS_LIMPO", orientation='h')
            fig.update_layout(yaxis={'categoryorder':'total ascending', 'title': None}, xaxis={'title': None})
            st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
            st.download_button("📥 Baixar Status", converter_csv(df_status), "status_uf.csv")
        else: st.info("Sem dados de status.")

    st.divider()

    # --- 4. Produtos e Municípios ---
    col_prod, col_mun = st.columns(2)
    with col_prod:
        st.subheader("📦 Top Produtos")
        if not df_prod.empty:
            top_p = df_prod.head(10).sort_values('Quantidade', ascending=True)
            col_y = 'COD_PRODUTO' if 'COD_PRODUTO' in top_p.columns else top_p.columns[0]
            fig_p = px.bar(top_p, x='Quantidade', y=col_y, orientation='h', text_auto=True)
            fig_p.update_layout(yaxis_title=None, xaxis_title=None)
            st.plotly_chart(padronizar_grafico(fig_p, tema), use_container_width=True)
            st.download_button("📥 Baixar Produtos", converter_csv(df_prod), "top_produtos.csv", key="bp")
        else: st.info("Sem dados de produtos.")

    with col_mun:
        st.subheader("🏙️ Top Municípios")
        if not df_mun.empty:
            top_m = df_mun.head(10).sort_values('Quantidade', ascending=True)
            fig_m = px.bar(top_m, x='Quantidade', y='Municipio', orientation='h', text_auto=True, color_discrete_sequence=['#00CC96'])
            fig_m.update_layout(yaxis_title=None, xaxis_title=None)
            st.plotly_chart(padronizar_grafico(fig_m, tema), use_container_width=True)
            st.download_button("📥 Baixar Municípios", converter_csv(df_mun), "top_municipios.csv", key="bm")
        else: st.info("Sem dados de municípios.")

    st.divider()
    
    # --- 5. Esfera e Tabela ---
    c_esf, c_tab = st.columns([1, 2])
    with c_esf:
        st.subheader("🏛️ Por Esfera")
        if not df_ativos.empty:
            df_esf = df_ativos['ESFERA_LIMPA'].value_counts().reset_index()
            df_esf.columns = ['Esfera', 'Qtd']
            fig = px.pie(df_esf, values='Qtd', names='Esfera', hole=0.5)
            st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        else: st.info("Sem dados de esfera.")

    with c_tab:
        st.subheader("📋 Tabela de Órgãos (Filtrada)")
        if not df_ativos.empty:
            cols = [c for c in ['NOME', 'UF', 'MUNICIPIO', 'ESFERA_LIMPA'] if c in df_ativos.columns]
            df_show = df_ativos[cols].copy()
            st.dataframe(df_show, use_container_width=True, height=300)
            st.download_button("📥 Baixar Tabela", converter_csv(df_show), "orgaos_filtrados.csv")
        else: st.info("Tabela vazia.")

    # --- 6. REDE DE COLABORADORES ---
    st.divider()
    st.markdown("### 👥 Pontos Focais (Rede PNATRANS)")
    
    if not df_users.empty:
        u1, u2 = st.columns(2)
        with u1:
            st.markdown("**Distribuição por Instituição**")
            if 'ORGAO' in df_users.columns:
                df_org_count = df_users['ORGAO'].value_counts().reset_index()
                df_org_count.columns = ['Órgão', 'Pontos Focais']
                fig_users = px.bar(df_org_count.head(10), x='Pontos Focais', y='Órgão', orientation='h', 
                                   text_auto=True, color='Pontos Focais', color_continuous_scale='Teal')
                fig_users.update_layout(yaxis=dict(autorange="reversed"), yaxis_title=None)
                st.plotly_chart(padronizar_grafico(fig_users, tema), use_container_width=True)
            else: st.warning("Coluna 'ORGAO' não encontrada.")

        with u2:
            st.markdown("**Pontos Focais por UF**")
            if 'UF' in df_users.columns:
                df_uf_count = df_users['UF'].value_counts().reset_index()
                df_uf_count.columns = ['UF', 'Qtd']
                df_uf_count = df_uf_count.sort_values('Qtd', ascending=False)
                fig_uf = px.bar(df_uf_count, x='UF', y='Qtd', text_auto=True, color='Qtd', color_continuous_scale='Blues')
                fig_uf.update_layout(xaxis_title=None, yaxis_title="Qtd")
                st.plotly_chart(padronizar_grafico(fig_uf, tema), use_container_width=True)
            else: st.info("Coluna 'UF' não encontrada.")
        
        with st.expander("📋 Ver Lista de Pontos Focais"):
            cols_hide = ['SENHA', 'PASSWORD', 'ID', 'TOKEN', 'CRIADO_EM', 'ATUALIZADO_EM']
            cols_show = [c for c in df_users.columns if c not in cols_hide]
            st.dataframe(df_users[cols_show], use_container_width=True)
    else: st.info("Nenhum 'Ponto Focal' encontrado.")

def render_analise_temporal(df_raw, tema):
    st.markdown("### 📈 Evolução de Produtos")
    if df_raw.empty:
        st.warning("Sem dados.")
        return
    col_data = next((c for c in df_raw.columns if 'DATA' in c.upper() or 'CRIADO' in c.upper()), None)
    if col_data:
        df = df_raw.copy()
        df['DT'] = pd.to_datetime(df[col_data], errors='coerce', dayfirst=True).dropna()
        if 'TIPO_FONTE' in df.columns: df = df[df['TIPO_FONTE'] == 'REALIZADO']
        if not df.empty:
            df['Mes'] = df['DT'].dt.to_period('M').astype(str)
            df_g = df.groupby('Mes').size().reset_index(name='Qtd').sort_values('Mes')
            fig = px.line(df_g, x='Mes', y='Qtd', markers=True)
            st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)