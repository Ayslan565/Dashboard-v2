import streamlit as st
import plotly.express as px
import pandas as pd
from utils import html_card, padronizar_grafico, converter_csv, carregar_geojson

def render_visao_geral(df_mapa, df_org, df_prod, df_status, tema, df_mun, df_users=pd.DataFrame()):
    st.markdown(f"### 📊 Visão Geral Completa")
    
    # --- 1. TRATAMENTO DE DADOS DOS USUÁRIOS (FILTROS) ---
    if not df_users.empty:
        # Padroniza colunas para maiúsculo
        df_users.columns = [c.upper() for c in df_users.columns]
        
        # FILTRO SOLICITADO: Manter APENAS 'PONTO FOCAL'
        # Isso remove automaticamente Master, Aprovador, Perfil N, etc.
        if 'PERFIL' in df_users.columns:
            df_users = df_users[df_users['PERFIL'].astype(str).str.contains("PONTO FOCAL", na=False, case=False)]

    # Prepara dados de órgãos ativos (Produtos)
    df_ativos = df_org[df_org['ENVIOU_PRODUTO'] == 'SIM'] if not df_org.empty else pd.DataFrame()
    
    # --- 2. KPIs ---
    c1, c2, c3, c4, c5 = st.columns(5)
    
    total_produtos = int(df_mapa['Total'].sum()) if not df_mapa.empty else 0
    total_orgaos = len(df_ativos)
    total_federais = len(df_ativos[df_ativos['ESFERA_LIMPA']=='FEDERAL']) if not df_ativos.empty else 0
    total_estaduais = len(df_ativos[df_ativos['ESFERA_LIMPA']=='ESTADUAL']) if not df_ativos.empty else 0
    total_municipais = len(df_ativos[df_ativos['ESFERA_LIMPA']=='MUNICIPAL']) if not df_ativos.empty else 0

    with c1: 
        st.markdown(html_card("📦 Produtos", total_produtos, "Total Recebido", tema), unsafe_allow_html=True)
        if not df_prod.empty:
            st.download_button("📥 CSV", converter_csv(df_prod), "produtos.csv", key="b1")
    with c2: 
        st.markdown(html_card("🏢 Órgãos", total_orgaos, "Ativos", tema), unsafe_allow_html=True)
        if not df_ativos.empty:
            st.download_button("📥 CSV", converter_csv(df_ativos), "ativos.csv", key="b2")
    with c3: 
        st.markdown(html_card("🏛️ Federais", total_federais, "Ativos", tema), unsafe_allow_html=True)
    with c4: 
        st.markdown(html_card("🗺️ Estaduais", total_estaduais, "Ativos", tema), unsafe_allow_html=True)
    with c5: 
        st.markdown(html_card("🏙️ Municipais", total_municipais, "Ativos", tema), unsafe_allow_html=True)

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
            else:
                st.info("Sem dados para o mapa.")
        except: st.warning("Mapa carregando...")

    with c_status:
        st.subheader("🚦 Status por UF")
        if not df_status.empty:
            fig = px.bar(df_status, x="Quantidade", y="UF_LIMPA", color="STATUS_LIMPO", orientation='h')
            fig.update_layout(yaxis={'categoryorder':'total ascending', 'title': None}, xaxis={'title': None})
            st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
            st.download_button("📥 Baixar Status", converter_csv(df_status), "status_uf.csv")
        else:
            st.info("Sem dados de status.")

    st.divider()

    # --- 4. Produtos e Municípios ---
    col_prod, col_mun = st.columns(2)
    
    with col_prod:
        st.subheader("📦 Top Produtos")
        if not df_prod.empty:
            top_p = df_prod.head(10).sort_values('Quantidade', ascending=True)
            if 'COD_PRODUTO' in top_p.columns:
                fig_p = px.bar(top_p, x='Quantidade', y='COD_PRODUTO', orientation='h', 
                               text_auto=True, hover_data={'DESC_PRODUTO':True})
            elif 'Nome_Produto' in top_p.columns:
                fig_p = px.bar(top_p, x='Quantidade', y='Nome_Produto', orientation='h', text_auto=True)
            else:
                fig_p = px.bar(top_p, x='Quantidade', y=top_p.columns[0], orientation='h')
                
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
        else:
            st.info("Sem dados de esfera.")

    with c_tab:
        st.subheader("📋 Tabela de Órgãos")
        if not df_ativos.empty:
            cols = [c for c in ['NOME', 'UF', 'MUNICIPIO', 'ESFERA_LIMPA'] if c in df_ativos.columns]
            df_show = df_ativos[cols].copy()
            st.dataframe(df_show, use_container_width=True, height=300)
            st.download_button("📥 Baixar Tabela", converter_csv(df_show), "orgaos_filtrados.csv")
        else:
            st.info("Tabela vazia.")

    # --- 6. REDE DE COLABORADORES (PONTOS FOCAIS) ---
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
                fig_users.update_layout(yaxis=dict(autorange="reversed"), yaxis_title=None, xaxis_title=None)
                st.plotly_chart(padronizar_grafico(fig_users, tema), use_container_width=True)
            else:
                st.warning("Coluna 'ORGAO' não encontrada.")

        with u2:
            st.markdown("**Pontos Focais por Estado (UF)**")
            if 'UF' in df_users.columns:
                # Conta usuários por UF e ordena
                df_uf_count = df_users['UF'].value_counts().reset_index()
                df_uf_count.columns = ['UF', 'Qtd']
                df_uf_count = df_uf_count.sort_values('Qtd', ascending=False)

                # Gráfico de Colunas Verticais
                fig_uf = px.bar(df_uf_count, x='UF', y='Qtd', text_auto=True, 
                                color='Qtd', color_continuous_scale='Blues')
                
                fig_uf.update_layout(xaxis_title=None, yaxis_title="Quantidade")
                st.plotly_chart(padronizar_grafico(fig_uf, tema), use_container_width=True)
            else:
                st.info("Coluna 'UF' não encontrada nos dados de usuários.")
        
        with st.expander("📋 Ver Lista de Pontos Focais"):
            # Oculta colunas sensíveis
            cols_hide = ['SENHA', 'PASSWORD', 'ID', 'TOKEN', 'CRIADO_EM', 'ATUALIZADO_EM']
            cols_show = [c for c in df_users.columns if c not in cols_hide]
            st.dataframe(df_users[cols_show], use_container_width=True)
    else:
        st.info("Nenhum 'Ponto Focal' encontrado na base de dados.")

def render_analise_temporal(df_raw, tema):
    st.markdown("### 📈 Evolução de Produtos")
    if df_raw.empty:
        st.warning("Sem dados temporais.")
        return

    col_data = next((c for c in df_raw.columns if 'DATA' in c.upper() or 'CRIADO' in c.upper()), None)
    if col_data:
        df = df_raw.copy()
        df['DT'] = pd.to_datetime(df[col_data], errors='coerce', dayfirst=True)
        df = df.dropna(subset=['DT'])
        if 'TIPO_FONTE' in df.columns: df = df[df['TIPO_FONTE'] == 'REALIZADO']
        
        if not df.empty:
            df['Mes'] = df['DT'].dt.to_period('M').astype(str)
            df_g = df.groupby('Mes').size().reset_index(name='Qtd').sort_values('Mes')
            
            fig = px.line(df_g, x='Mes', y='Qtd', markers=True)
            st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)