import streamlit as st
import plotly.express as px
import pandas as pd
from utils import html_card, padronizar_grafico, converter_csv, carregar_geojson, carregar_capacitacoes

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

    # Prepara dados de órgãos ativos
    df_ativos = df_org[df_org['ENVIOU_PRODUTO'] == 'SIM'] if not df_org.empty else pd.DataFrame()
    
    # Classificação Iniciativa Privada
    termos_privados = ['ABCR', 'ABEETRANS', 'ABNT', 'ABRAPSIT', 'ABSEV', 'AEA', 'ANFAVEA', 'ARTERIS', 'LOCADORAS', 'CIDADEAPÉ', 'CNC', 'CNT', 'CNVV', 'FENATRAL', 'FENIVE', 'IBDTRANSITO', 'CORDIAL', 'INPROTRAN', 'HONDA', 'YAMAHA', 'NITERÓI TRÂNSITO', 'REDE EDUCATIVA', 'SEST SENAT', 'UCB', 'EMPRESA PRIVADA']
    df_privada = pd.DataFrame()
    if not df_ativos.empty:
        mask_termo = df_ativos['NOME'].astype(str).str.upper().apply(lambda x: any(termo in x for termo in termos_privados))
        mask_esfera = df_ativos['ESFERA_LIMPA'].isin(['PRIVADA', 'PARTICULAR', 'INICIATIVA PRIVADA', 'EMPRESA'])
        df_privada = df_ativos[mask_termo | mask_esfera]

    # --- 2. KPIs ---
    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
    
    total_produtos = int(df_mapa['Total'].sum()) if not df_mapa.empty else 0
    total_geral_orgaos = len(df_org)
    total_ativos = len(df_ativos)
    
    # Filtros para download
    df_federal = df_ativos[df_ativos['ESFERA_LIMPA']=='FEDERAL'] if not df_ativos.empty else pd.DataFrame()
    df_estadual = df_ativos[df_ativos['ESFERA_LIMPA']=='ESTADUAL'] if not df_ativos.empty else pd.DataFrame()
    df_municipal = df_ativos[df_ativos['ESFERA_LIMPA']=='MUNICIPAL'] if not df_ativos.empty else pd.DataFrame()

    with c1: 
        st.markdown(html_card("📦 Produtos", total_produtos, "Recebidos", tema), unsafe_allow_html=True)
        if not df_prod.empty: st.download_button("📥 CSV", converter_csv(df_prod), "produtos.csv", key="b1")
    with c2:
        st.markdown(html_card("🏢 Total", total_geral_orgaos, "Cadastrados", tema), unsafe_allow_html=True)
        if not df_org.empty: st.download_button("📥 CSV", converter_csv(df_org), "lista_completa.csv", key="b_total")
    with c3: 
        st.markdown(html_card("✅ Ativos", total_ativos, "Enviaram", tema), unsafe_allow_html=True)
        if not df_ativos.empty: st.download_button("📥 CSV", converter_csv(df_ativos), "ativos.csv", key="b2")
    with c4: 
        st.markdown(html_card("🏛️ Federais", len(df_federal), "Ativos", tema), unsafe_allow_html=True)
        if not df_federal.empty: st.download_button("📥 CSV", converter_csv(df_federal), "federais.csv", key="b3")
    with c5: 
        st.markdown(html_card("🗺️ Estaduais", len(df_estadual), "Ativos", tema), unsafe_allow_html=True)
        if not df_estadual.empty: st.download_button("📥 CSV", converter_csv(df_estadual), "estaduais.csv", key="b4")
    with c6: 
        st.markdown(html_card("🏙️ Municipais", len(df_municipal), "Ativos", tema), unsafe_allow_html=True)
        if not df_municipal.empty: st.download_button("📥 CSV", converter_csv(df_municipal), "municipais.csv", key="b5")
    with c7:
        st.markdown(html_card("🏭 Privadas", len(df_privada), "Iniciativas", tema), unsafe_allow_html=True)
        if not df_privada.empty: st.download_button("📥 CSV", converter_csv(df_privada), "privadas.csv", key="b6")

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
        except: st.warning("Carregando mapa...")

    with c_status:
        st.subheader("🚦 Status por UF")
        if not df_status.empty:
            fig = px.bar(df_status, x="Quantidade", y="UF_LIMPA", color="STATUS_LIMPO", orientation='h')
            fig.update_layout(yaxis={'categoryorder':'total ascending', 'title': None}, xaxis={'title': None})
            st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
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
        else: st.info("Sem dados.")

    with col_mun:
        st.subheader("🏙️ Top Municípios")
        if not df_mun.empty:
            top_m = df_mun.head(10).sort_values('Quantidade', ascending=True)
            fig_m = px.bar(top_m, x='Quantidade', y='Municipio', orientation='h', text_auto=True, color_discrete_sequence=['#00CC96'])
            fig_m.update_layout(yaxis_title=None, xaxis_title=None)
            st.plotly_chart(padronizar_grafico(fig_m, tema), use_container_width=True)
        else: st.info("Sem dados.")

    st.divider()
    
    # --- 5. ESFERAS E TABELA ---
    c_esf, c_tab = st.columns([1, 2])
    with c_esf:
        st.subheader("🏛️ Por Esfera")
        if not df_ativos.empty:
            df_esf = df_ativos['ESFERA_LIMPA'].value_counts().reset_index()
            df_esf.columns = ['Esfera', 'Qtd']
            fig = px.pie(df_esf, values='Qtd', names='Esfera', hole=0.5)
            st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
    with c_tab:
        st.subheader("📋 Tabela de Órgãos (Filtrada)")
        if not df_ativos.empty:
            cols = [c for c in ['NOME', 'UF', 'MUNICIPIO', 'ESFERA_LIMPA'] if c in df_ativos.columns]
            st.dataframe(df_ativos[cols], use_container_width=True, height=300)
        else: st.info("Tabela vazia.")

    st.divider()
    
    # =========================================================================
    # --- 6. CAPACITAÇÕES E TREINAMENTOS (INTEGRADO NO PAINEL) ---
    # =========================================================================
    st.markdown("### 🎓 Capacitações e Treinamentos Realizados")
    
    # Carrega dados do Banco (tabela capacitacoes)
    df_cap = carregar_capacitacoes()
    
    if not df_cap.empty:
        # Métricas Rápidas
        total_caps = len(df_cap)
        total_parts = int(df_cap['QTD_PARTICIPANTES'].sum())
        
        # Pega a data mais recente
        if 'DATA_CAPACITACAO' in df_cap.columns:
            ult_data = pd.to_datetime(df_cap['DATA_CAPACITACAO']).max().strftime('%d/%m/%Y')
        else:
            ult_data = "-"
            
        # Cards de Capacitação
        k1, k2, k3 = st.columns(3)
        with k1: st.markdown(html_card("Eventos", total_caps, "Realizados", tema), unsafe_allow_html=True)
        with k2: st.markdown(html_card("Participantes", f"{total_parts:,}", "Total Capacitados", tema), unsafe_allow_html=True)
        with k3: st.markdown(html_card("Último Evento", ult_data, "Data Recente", tema), unsafe_allow_html=True)

        st.markdown("##### 📋 Histórico Detalhado")
        
        # Tratamento para Tabela
        df_show = df_cap.copy()
        if 'DATA_CAPACITACAO' in df_show.columns:
            df_show['Data'] = pd.to_datetime(df_show['DATA_CAPACITACAO']).dt.strftime('%d/%m/%Y')
        else: df_show['Data'] = "-"

        # Renomeia colunas para ficar bonito
        rename_map = {
            'ORDEM': 'Ordem',
            'DESCRICAO': 'Descrição do Evento',
            'TIPO': 'Tipo / Abrangência',
            'LISTA_PRESENCA': 'Lista Presença',
            'QTD_PARTICIPANTES': 'Participantes'
        }
        
        cols_order = ['Ordem', 'Data', 'Descrição do Evento', 'Participantes', 'Tipo / Abrangência', 'Lista Presença']
        df_table = df_show.rename(columns=rename_map)
        
        # Filtra colunas existentes
        cols_final = [c for c in cols_order if c in df_table.columns]
        
        st.dataframe(
            df_table[cols_final],
            use_container_width=True,
            hide_index=True,
            column_config={
                "Participantes": st.column_config.NumberColumn(format="%d"),
                "Ordem": st.column_config.NumberColumn(format="%d")
            }
        )
    else:
        st.info("Nenhuma capacitação registrada no sistema.")

    st.divider()

    # --- 7. REDE DE COLABORADORES ---
    st.markdown("### 👥 Pontos Focais (Rede PNATRANS)")
    
    if not df_users.empty:
        u1, u2 = st.columns(2)
        with u1:
            st.markdown("**Distribuição por Instituição**")
            if 'ORGAO' in df_users.columns:
                df_c = df_users['ORGAO'].value_counts().reset_index()
                df_c.columns = ['Órgão', 'Qtd']
                fig = px.bar(df_c.head(10), x='Qtd', y='Órgão', orientation='h', text_auto=True, color='Qtd', color_continuous_scale='Teal')
                fig.update_layout(yaxis=dict(autorange="reversed"), yaxis_title=None)
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        with u2:
            st.markdown("**Pontos Focais por UF**")
            if 'UF' in df_users.columns:
                df_u = df_users['UF'].value_counts().reset_index()
                df_u.columns = ['UF', 'Qtd']
                fig = px.bar(df_u.sort_values('Qtd', ascending=False), x='UF', y='Qtd', text_auto=True, color='Qtd', color_continuous_scale='Blues')
                fig.update_layout(xaxis_title=None)
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        
        with st.expander("📋 Ver Lista de Pontos Focais"):
            cols_hide = ['SENHA', 'PASSWORD', 'ID', 'TOKEN', 'CRIADO_EM', 'ATUALIZADO_EM']
            cols = [c for c in df_users.columns if c not in cols_hide]
            st.dataframe(df_users[cols], use_container_width=True)
    else: st.info("Nenhum ponto focal encontrado.")

# =========================================================================
# --- 8. ANÁLISE TEMPORAL (MÊS E ANO COM MAIS PRODUTOS) ---
# =========================================================================
def render_analise_temporal(df_raw, tema):
    st.markdown("### 📈 Evolução de Cadastros (Meses e Anos com mais entregas)")
    
    if df_raw.empty:
        st.warning("Sem dados para exibir na evolução.")
        return
        
    if 'DATA_CADASTRO' in df_raw.columns:
        df = df_raw.copy()
        
        # Converte para data verdadeira, forçando o formato brasileiro (dia/mês/ano)
        df['DT'] = pd.to_datetime(df['DATA_CADASTRO'], errors='coerce', dayfirst=True).dropna()
        
        # 🚨 FILTRO ANTIBUG: Remove qualquer data que seja maior que a data de hoje (Futuro)
        hoje = pd.Timestamp.today()
        df = df[df['DT'] <= hoje]
        
        # Filtra apenas o que já foi REALIZADO
        if 'STATUS_LIMPO' in df.columns: 
            df = df[df['STATUS_LIMPO'] == 'REALIZADO'] 
            
        if not df.empty:
            # Dicionário de tradução dos meses
            meses_pt = {
                1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
                5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
                9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
            }
            
            # Cria a coluna invisível para ordenar cronologicamente (ex: '2023-10')
            df['Ordem_Mes'] = df['DT'].dt.to_period('M').astype(str)
            
            # Cria a coluna bonita para exibir no eixo X (ex: 'Janeiro de 2025')
            df['Mes_Exibicao'] = df['DT'].dt.month.map(meses_pt) + ' de ' + df['DT'].dt.year.astype(str)
            
            # Agrupa os dados
            df_g = df.groupby(['Ordem_Mes', 'Mes_Exibicao']).size().reset_index(name='Qtd').sort_values('Ordem_Mes')
            
            # Gráfico de Barras colorido evidenciando o volume
            fig = px.bar(df_g, x='Mes_Exibicao', y='Qtd', text_auto=True, 
                         color='Qtd', color_continuous_scale='Blues',
                         labels={'Mes_Exibicao': 'Período', 'Qtd': 'Produtos Entregues'})
            
            fig.update_layout(
                xaxis_title="Mês e Ano", 
                yaxis_title="Quantidade de Produtos",
                xaxis={
                    'type': 'category',
                    'categoryorder': 'array',
                    'categoryarray': df_g['Mes_Exibicao'] # Força a ordem exata
                } 
            )
            
            st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        else:
            st.info("Nenhum produto com data de cadastro válida no passado foi encontrado.")
    else:
        st.warning("A coluna 'DATA_CADASTRO' não foi encontrada. Verifique se o ETL rodou com sucesso.")