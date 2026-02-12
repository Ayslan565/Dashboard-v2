import streamlit as st
import plotly.express as px
import pandas as pd
from utils import html_card, padronizar_grafico, converter_csv

def render_prf(df, tema):
    st.markdown("### 🚗 PRF - Monitoramento Avançado de Sinistros")
    
    if df.empty: 
        st.error("⚠️ Base de dados vazia. Verifique a conexão.")
        return

    # --- BARRA LATERAL: FILTROS ---
    st.sidebar.divider()
    st.sidebar.subheader("Filtros")
    
    # 1. Ano
    anos = sorted(df['ANO'].unique(), reverse=True)
    sel_anos = st.sidebar.multiselect("📅 Ano:", anos, default=[anos[0]] if anos else [])
    
    # 2. Métrica de Visualização (MOVIDO PARA CÁ)
    tipo_metrica = st.sidebar.radio("📊 Métrica dos Rankings:", ["Absoluto (Total)", "Taxa (por 1.000 hab.)"])

    # 3. Estado Físico
    if 'ESTADO_FISICO' in df.columns:
        opcoes_fisico = sorted([x for x in df['ESTADO_FISICO'].astype(str).unique() if x != 'nan'])
        sel_fisico = st.sidebar.multiselect("🏥 Estado Físico (Vítima):", opcoes_fisico, placeholder="Todos (Padrão)")
    else:
        sel_fisico = []

    # 4. Estado (UF)
    ufs_list = sorted(df['UF'].astype(str).unique())
    sel_ufs = st.sidebar.multiselect("🗺️ Estado (UF):", ufs_list, placeholder="Todos (Brasil)")

    # Filtragem preliminar
    df_temp = df
    if sel_anos: df_temp = df_temp[df_temp['ANO'].isin(sel_anos)]
    if sel_ufs: df_temp = df_temp[df_temp['UF'].isin(sel_ufs)]
    
    # 5. Rodovia
    brs_disponiveis = sorted(df_temp['BR'].astype(str).unique())
    sel_brs = st.sidebar.multiselect("🛣️ Rodovia (BR):", brs_disponiveis[:200])

    # --- APLICAÇÃO FINAL DOS FILTROS ---
    df_f = df.copy()
    if sel_anos: df_f = df_f[df_f['ANO'].isin(sel_anos)]
    if sel_ufs: df_f = df_f[df_f['UF'].isin(sel_ufs)]
    if sel_brs: df_f = df_f[df_f['BR'].astype(str).isin(sel_brs)]
    if sel_fisico: df_f = df_f[df_f['ESTADO_FISICO'].astype(str).isin(sel_fisico)]

    # --- KPIs GERAIS ---
    k1, k2, k3, k4 = st.columns(4)
    total_pessoas = len(df_f) 
    total_sinistros = df_f['ID'].nunique() if 'ID' in df_f.columns else len(df_f)
    mortos = int(df_f['MORTOS'].sum())
    feridos = int(df_f['FERIDOS'].sum())
    sev = (mortos / total_sinistros * 100) if total_sinistros > 0 else 0
    
    with k1: st.markdown(html_card("Sinistros", f"{total_sinistros:,}", "Ocorrências Únicas", tema), unsafe_allow_html=True)
    with k2: st.markdown(html_card("Envolvidos", f"{total_pessoas:,}", "Total de Pessoas", tema), unsafe_allow_html=True)
    with k3: st.markdown(html_card("Óbitos", f"{mortos:,}", "Vítimas Fatais", tema), unsafe_allow_html=True)
    with k4: st.markdown(html_card("Índice Severidade", f"{sev:.1f}", "Mortos / 100 Sinistros", tema), unsafe_allow_html=True)

    st.divider()
    
    # --- ÁREA DE ANÁLISE ---
    tabs = st.tabs(["👥 Perfil Vítimas", "🚗 Veículos & Frota", "📍 Localização & Taxas", "⚠️ Causas & Contexto", "🗺️ Mapa Geo"])
    
    # ABA 1: PERFIL VÍTIMAS
    with tabs[0]:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Gênero")
            if 'SEXO' in df_f.columns:
                df_s = df_f[~df_f['SEXO'].isin(['NÃO INFORMADO', 'Igno', 'Inválido'])]
                if not df_s.empty:
                    fig = px.pie(df_s['SEXO'].value_counts().reset_index(), values='count', names='SEXO', hole=0.5, color_discrete_sequence=px.colors.qualitative.Pastel)
                    st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        with c2:
            st.subheader("Estado Físico")
            if 'ESTADO_FISICO' in df_f.columns:
                df_e = df_f[~df_f['ESTADO_FISICO'].isin(['NÃO INFORMADO', 'Igno'])]
                if not df_e.empty:
                    fig = px.bar(df_e['ESTADO_FISICO'].value_counts().reset_index(), x='count', y='ESTADO_FISICO', orientation='h', text_auto=True, color='count', color_continuous_scale='Reds')
                    fig.update_layout(yaxis=dict(autorange="reversed"))
                    st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        
        st.subheader("Distribuição Etária")
        if 'IDADE' in df_f.columns:
            df_i = df_f[(df_f['IDADE'] > 0) & (df_f['IDADE'] < 110)]
            if not df_i.empty:
                fig = px.histogram(df_i, x="IDADE", nbins=50, color_discrete_sequence=['#2196F3'], text_auto=True)
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

    # ABA 2: VEÍCULOS & FROTA
    with tabs[1]:
        c_veic, c_ano = st.columns(2)
        with c_veic:
            st.subheader("Participação por Tipo de Veículo")
            if 'TIPO_VEICULO' in df_f.columns:
                top_v = df_f['TIPO_VEICULO'].value_counts().head(10).reset_index()
                fig = px.bar(top_v, x='count', y='TIPO_VEICULO', orientation='h', text_auto=True, color='count')
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        with c_ano:
            st.subheader("Idade da Frota")
            if 'ANO_FABRICACAO_VEICULO' in df_f.columns:
                df_ano = df_f[(df_f['ANO_FABRICACAO_VEICULO'] > 1980) & (df_f['ANO_FABRICACAO_VEICULO'] <= 2026)]
                fig = px.histogram(df_ano, x="ANO_FABRICACAO_VEICULO", nbins=20, text_auto=True)
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        
        st.divider()
        st.markdown("### ☠️ Ranking de Letalidade (Óbitos por Categoria)")
        t_moto, t_motoneta, t_carro, t_pesado, t_bus = st.tabs(["🏍️ Motos", "🛵 Motonetas", "🚗 Carros", "🚛 Pesados", "🚌 Ônibus"])

        if 'MARCA' in df_f.columns and 'TIPO_VEICULO' in df_f.columns:
            df_fatal = df_f[(df_f['MORTOS'] > 0) | (df_f['ESTADO_FISICO'].astype(str).str.upper().isin(['ÓBITO', 'MORTO', 'FATAL']))].copy()
            df_fatal = df_fatal[~df_fatal['MARCA'].astype(str).str.upper().isin(['NÃO INFORMADO', 'OUTRA', 'NI', 'NI/NI', 'S/M'])]

            def plot_ranking(df_source, regex, cor):
                mask = df_source['TIPO_VEICULO'].astype(str).str.upper().str.contains(regex)
                ranking = df_source[mask]['MARCA'].value_counts().head(15).reset_index()
                if ranking.empty: 
                    st.info("Sem dados suficientes.")
                    return
                fig = px.bar(ranking, x='count', y='MARCA', orientation='h', text_auto=True, color='count', color_continuous_scale=cor)
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

            with t_moto: plot_ranking(df_fatal, 'MOTOCICLETA', 'Reds')
            with t_motoneta: plot_ranking(df_fatal, 'MOTONETA|CICLOMOTOR', 'Purples')
            with t_carro: plot_ranking(df_fatal, 'AUTOM|CARRO|CAMIONETA', 'Blues')
            with t_pesado: plot_ranking(df_fatal, 'CAMINH|TRATOR', 'Oranges')
            with t_bus: plot_ranking(df_fatal, 'ONIBUS|MICRO', 'Greens')

    # ABA 3: LOCALIZAÇÃO & TAXAS (REORGANIZADO - UM ABAIXO DO OUTRO)
    with tabs[2]:
        # Carregamento de População (Controlado pelo filtro lateral)
        df_pop = pd.DataFrame()
        if tipo_metrica == "Taxa (por 1.000 hab.)":
            from utils import carregar_populacao
            df_pop = carregar_populacao()
            if df_pop.empty:
                st.warning("⚠️ Dados de população não disponíveis no banco.")
                tipo_metrica = "Absoluto (Total)"

        # 1. Gráfico Empilhado (Estados x Veículos)
        st.markdown("##### 🚗 Composição da Frota Acidentada por UF")
        if 'TIPO_VEICULO' in df_f.columns:
            top_tipos = ['MOTOCICLETA', 'AUTOMÓVEL', 'CAMINHÃO', 'CAMINHONETE', 'ÔNIBUS', 'MOTONETA']
            df_s = df_f.copy()
            df_s['TIPO_V'] = df_s['TIPO_VEICULO'].str.upper()
            df_s.loc[~df_s['TIPO_V'].apply(lambda x: any(t in str(x) for t in top_tipos)), 'TIPO_V'] = 'OUTROS'
            top_ufs = df_s['UF'].value_counts().head(15).index
            df_g = df_s[df_s['UF'].isin(top_ufs)].groupby(['UF', 'TIPO_V']).size().reset_index(name='Qtd')
            fig_s = px.bar(df_g, x='Qtd', y='UF', color='TIPO_V', orientation='h', barmode='stack')
            fig_s.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(padronizar_grafico(fig_s, tema), use_container_width=True)

        st.divider()
        
        # --- RANKING DE ESTADOS (GRANDE / FULL WIDTH) ---
        st.markdown(f"### 🗺️ Ranking por Estado ({tipo_metrica})")
        df_uf = df_f['UF'].value_counts().reset_index(name='Qtd').rename(columns={'index':'UF'})
        
        if tipo_metrica == "Taxa (por 1.000 hab.)" and not df_pop.empty:
            pop_uf = df_pop.groupby('uf_norm')['populacao'].sum().reset_index()
            df_m = pd.merge(df_uf, pop_uf, left_on='UF', right_on='uf_norm')
            df_m['Valor'] = (df_m['Qtd'] / df_m['populacao']) * 1000
            # Altura ajustada para caber todos os estados
            fig = px.bar(df_m.sort_values('Valor', ascending=False).head(30), x='Valor', y='UF', orientation='h', text_auto='.2f', color='Valor', height=700)
        else:
            fig = px.bar(df_uf.head(30), x='Qtd', y='UF', orientation='h', text_auto=True, color='Qtd', height=700)
        
        fig.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

        st.divider()

        # --- RANKING DE MUNICÍPIOS (GRANDE / FULL WIDTH) ---
        st.markdown(f"### 🏙️ Top 30 Municípios ({tipo_metrica})")
        df_m_c = df_f.groupby(['MUNICIPIO', 'UF']).size().reset_index(name='Qtd')
        
        if tipo_metrica == "Taxa (por 1.000 hab.)" and not df_pop.empty:
            df_m_c['mun_n'] = df_m_c['MUNICIPIO'].str.upper().str.strip()
            df_m2 = pd.merge(df_m_c, df_pop, left_on=['mun_n', 'UF'], right_on=['municipio_norm', 'uf_norm'])
            df_m2 = df_m2[df_m2['populacao'] > 5000] # Filtro cidades muito pequenas
            df_m2['Valor'] = (df_m2['Qtd'] / df_m2['populacao']) * 1000
            df_m2['Label'] = df_m2['MUNICIPIO'] + "-" + df_m2['UF']
            
            fig = px.bar(df_m2.sort_values('Valor', ascending=False).head(30), x='Valor', y='Label', orientation='h', text_auto='.2f', color='Valor', height=800)
        else:
            df_m_c['Label'] = df_m_c['MUNICIPIO'] + "-" + df_m_c['UF']
            fig = px.bar(df_m_c.sort_values('Qtd', ascending=False).head(30), x='Qtd', y='Label', orientation='h', text_auto=True, color='Qtd', height=800)
        
        fig.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

    # ABA 4: CAUSAS
    with tabs[3]:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Causa Principal")
            if 'CAUSA_PRINCIPAL' in df_f.columns:
                top_c = df_f['CAUSA_PRINCIPAL'].value_counts().head(10).reset_index()
                fig = px.bar(top_c, x='count', y='CAUSA_PRINCIPAL', orientation='h', text_auto=True, color='count')
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        with c2:
            st.subheader("Condição Meteorológica")
            if 'CONDICAO_METEREOLOGICA' in df_f.columns:
                fig = px.pie(df_f['CONDICAO_METEREOLOGICA'].value_counts().reset_index(), values='count', names='CONDICAO_METEREOLOGICA', hole=0.5)
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        
        c_f, c_p = st.columns(2)
        with c_f:
            st.subheader("Fase do Dia")
            if 'FASE_DIA' in df_f.columns:
                fig = px.pie(df_f['FASE_DIA'].value_counts().reset_index(), values='count', names='FASE_DIA', hole=0.5)
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        with c_p:
            st.subheader("Tipo de Pista")
            if 'TIPO_PISTA' in df_f.columns:
                fig = px.bar(df_f['TIPO_PISTA'].value_counts().reset_index(), x='count', y='TIPO_PISTA', orientation='h', text_auto=True)
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

    # ABA 5: MAPA
    with tabs[4]:
        st.subheader("Mapa de Calor (Densidade de Ocorrências)")
        if 'LAT' in df_f.columns and 'LON' in df_f.columns:
            coords = df_f[(df_f['LAT'] != 0) & (df_f['LON'] != 0)]
            if not coords.empty:
                if len(coords) > 20000: coords = coords.sample(20000)
                st.caption(f"Exibindo amostra de {len(coords):,} pontos georreferenciados.")
                fig_map = px.density_mapbox(coords, lat='LAT', lon='LON', radius=5, zoom=3, center=dict(lat=-15.78, lon=-47.92), mapbox_style="carto-positron" if tema['bg_card'] == '#FFFFFF' else "carto-darkmatter")
                fig_map.update_layout(height=600, margin={"r":0,"t":0,"l":0,"b":0})
                st.plotly_chart(fig_map, use_container_width=True)
            else: st.warning("Sem coordenadas válidas registradas.")