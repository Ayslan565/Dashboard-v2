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
    
    anos = sorted(df['ANO'].unique(), reverse=True)
    sel_anos = st.sidebar.multiselect("📅 Ano:", anos, default=[anos[0]] if anos else [])
    
    ufs = sorted(df['UF'].astype(str).unique())
    sel_ufs = st.sidebar.multiselect("🗺️ Estado (UF):", ufs)

    df_temp = df
    if sel_anos: df_temp = df_temp[df_temp['ANO'].isin(sel_anos)]
    if sel_ufs: df_temp = df_temp[df_temp['UF'].isin(sel_ufs)]
    
    brs_disponiveis = sorted(df_temp['BR'].astype(str).unique())
    if len(brs_disponiveis) > 200: brs_disponiveis = brs_disponiveis[:200]
    
    sel_brs = st.sidebar.multiselect("🛣️ Rodovia (BR):", brs_disponiveis)

    # Aplicação Final dos Filtros
    df_f = df.copy()
    if sel_anos: df_f = df_f[df_f['ANO'].isin(sel_anos)]
    if sel_ufs: df_f = df_f[df_f['UF'].isin(sel_ufs)]
    if sel_brs: df_f = df_f[df_f['BR'].astype(str).isin(sel_brs)]

    # --- KPIs GERAIS ---
    k1, k2, k3, k4 = st.columns(4)
    total_pessoas = len(df_f) 
    total_sinistros = df_f['ID'].nunique() if 'ID' in df_f.columns else len(df_f)
    mortos = int(df_f['MORTOS'].sum())
    feridos = int(df_f['FERIDOS'].sum())
    
    sev = (mortos / total_sinistros * 100) if total_sinistros > 0 else 0
    
    with k1: st.markdown(html_card("Sinistros", f"{total_sinistros:,}", "Total Ocorrências", tema), unsafe_allow_html=True)
    with k2: st.markdown(html_card("Envolvidos", f"{total_pessoas:,}", "Pessoas Totais", tema), unsafe_allow_html=True)
    with k3: st.markdown(html_card("Óbitos", f"{mortos:,}", "Vítimas Fatais", tema), unsafe_allow_html=True)
    with k4: st.markdown(html_card("Índice Severidade", f"{sev:.1f}", "Mortos / 100 Sinistros", tema), unsafe_allow_html=True)

    st.divider()
    
    # --- ÁREA DE ANÁLISE ---
    tabs = st.tabs([
        "👥 Perfil Vítimas", 
        "🚗 Veículos & Frota", 
        "📍 Localização & Vias", 
        "⚠️ Causas & Contexto", 
        "🗺️ Mapa Geo"
    ])
    
    # ABA 1: PERFIL VÍTIMAS
    with tabs[0]:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Gênero")
            if 'SEXO' in df_f.columns:
                df_s = df_f[~df_f['SEXO'].isin(['NÃO INFORMADO', 'Igno', 'Inválido'])]
                top_s = df_s['SEXO'].value_counts().reset_index()
                top_s.columns = ['Sexo', 'Qtd']
                fig = px.pie(top_s, values='Qtd', names='Sexo', hole=0.5, color_discrete_sequence=px.colors.qualitative.Pastel)
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        with c2:
            st.subheader("Estado Físico")
            if 'ESTADO_FISICO' in df_f.columns:
                df_e = df_f[~df_f['ESTADO_FISICO'].isin(['NÃO INFORMADO', 'Igno'])]
                top_e = df_e['ESTADO_FISICO'].value_counts().reset_index()
                top_e.columns = ['Estado', 'Qtd']
                fig = px.bar(top_e, x='Qtd', y='Estado', orientation='h', text_auto=True, color='Qtd', color_continuous_scale='Reds')
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

        if 'IDADE' in df_f.columns:
            st.subheader("Distribuição Etária")
            df_i = df_f[(df_f['IDADE'] > 0) & (df_f['IDADE'] < 110)]
            if not df_i.empty:
                # ADICIONADO: text_auto=True para exibir rótulos
                fig = px.histogram(df_i, x="IDADE", nbins=50, color_discrete_sequence=['#2196F3'], text_auto=True)
                fig.update_layout(bargap=0.1)
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

    # ABA 2: VEÍCULOS & FROTA
    with tabs[1]:
        c_veic, c_ano = st.columns(2)
        with c_veic:
            st.subheader("Participação por Tipo de Veículo")
            if 'TIPO_VEICULO' in df_f.columns:
                top_v = df_f['TIPO_VEICULO'].value_counts().head(10).reset_index()
                top_v.columns = ['Veículo', 'Qtd']
                fig = px.bar(top_v, x='Qtd', y='Veículo', orientation='h', text_auto=True, color='Qtd')
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

        with c_ano:
            st.subheader("Idade da Frota")
            if 'ANO_FABRICACAO_VEICULO' in df_f.columns:
                df_ano = df_f[(df_f['ANO_FABRICACAO_VEICULO'] > 1980) & (df_f['ANO_FABRICACAO_VEICULO'] <= 2026)]
                # ADICIONADO: text_auto=True para exibir rótulos
                fig = px.histogram(df_ano, x="ANO_FABRICACAO_VEICULO", nbins=20, text_auto=True)
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        
        st.divider()
        st.markdown("### Ranking de Marcas/Modelos com Vítimas Fatais")
        
        # Abas Específicas
        t_moto, t_motoneta, t_carro, t_pesado, t_bus = st.tabs([
            "🏍️ Motocicletas", "🛵 Motonetas/Ciclomotores", "🚗 Automóveis", "🚛 Caminhões", "🚌 Ônibus"
        ])

        if 'MARCA' in df_f.columns and 'TIPO_VEICULO' in df_f.columns and 'ESTADO_FISICO' in df_f.columns:
            # Filtra ÓBITOS
            df_fatal = df_f[(df_f['MORTOS'] > 0) | (df_f['ESTADO_FISICO'].astype(str).str.upper().isin(['ÓBITO', 'MORTO', 'FATAL']))].copy()
            # Remove marcas genéricas
            blacklist = ['NÃO INFORMADO', 'OUTRA', 'OUTRAS', 'NI', 'NAO INFORMADO', 'SEM MARCA', 'NI/NI', 'S/M']
            df_fatal = df_fatal[~df_fatal['MARCA'].astype(str).str.upper().isin(blacklist)]

            def plot_ranking(df_source, regex_filtro, cor_escala, titulo):
                mask = df_source['TIPO_VEICULO'].astype(str).str.upper().str.contains(regex_filtro)
                df_filtrado = df_source[mask]
                
                if df_filtrado.empty:
                    st.info(f"Sem registros de óbitos para {titulo}.")
                    return
                
                ranking = df_filtrado['MARCA'].value_counts().head(15).reset_index()
                ranking.columns = ['Marca/Modelo', 'Acidentes Fatais']
                
                fig = px.bar(
                    ranking, x='Acidentes Fatais', y='Marca/Modelo', orientation='h',
                    text_auto=True, color='Acidentes Fatais', color_continuous_scale=cor_escala,
                    title=f"Top 15 - {titulo}"
                )
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

            with t_moto: plot_ranking(df_fatal, 'MOTOCICLETA', 'Reds', "Motocicletas")
            with t_motoneta: plot_ranking(df_fatal, 'MOTONETA|CICLOMOTOR', 'Purples', "Motonetas e Ciclomotores")
            with t_carro: plot_ranking(df_fatal, 'AUTOM|CARRO|CAMIONETA|UTILITARIO', 'Blues', "Automóveis")
            with t_pesado: plot_ranking(df_fatal, 'CAMINH|TRATOR|REBOQUE', 'Oranges', "Caminhões e Pesados")
            with t_bus: plot_ranking(df_fatal, 'ONIBUS|MICRO', 'Greens', "Ônibus")

    # ABA 3: LOCALIZAÇÃO
    with tabs[2]:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Por Estado")
            top_uf = df_f['UF'].value_counts().head(15).reset_index()
            top_uf.columns = ['UF', 'Qtd']
            fig = px.bar(top_uf, x='Qtd', y='UF', orientation='h', text_auto=True)
            fig.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        with c2:
            st.subheader("Por Município")
            top_mun = df_f['MUNICIPIO'].value_counts().head(15).reset_index()
            top_mun.columns = ['Município', 'Qtd']
            fig = px.bar(top_mun, x='Qtd', y='Município', orientation='h', text_auto=True)
            fig.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

    # ABA 4: CAUSAS
    with tabs[3]:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Causa Principal")
            if 'CAUSA_PRINCIPAL' in df_f.columns:
                top_c = df_f['CAUSA_PRINCIPAL'].value_counts().head(10).reset_index()
                top_c.columns = ['Causa', 'Qtd']
                fig = px.bar(top_c, x='Qtd', y='Causa', orientation='h', text_auto=True, color='Qtd')
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        with c2:
            st.subheader("Condição Meteorológica")
            if 'CONDICAO_METEREOLOGICA' in df_f.columns:
                top_cl = df_f['CONDICAO_METEREOLOGICA'].value_counts().reset_index()
                top_cl.columns = ['Condição', 'Qtd']
                fig = px.pie(top_cl, values='Qtd', names='Condição', hole=0.5)
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

        c_fase, c_pista = st.columns(2)
        with c_fase:
            st.subheader("Fase do Dia")
            if 'FASE_DIA' in df_f.columns:
                fig = px.pie(df_f['FASE_DIA'].value_counts().reset_index(), values='count', names='FASE_DIA', hole=0.5)
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        
        with c_pista:
            st.subheader("Tipo de Pista")
            if 'TIPO_PISTA' in df_f.columns:
                top_p = df_f['TIPO_PISTA'].value_counts().reset_index()
                fig = px.bar(top_p, x='count', y='TIPO_PISTA', orientation='h', text_auto=True)
                fig.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

    # ABA 5: MAPA
    with tabs[4]:
        st.subheader("Mapa de Calor")
        if 'LAT' in df_f.columns and 'LON' in df_f.columns:
            coords = df_f[(df_f['LAT'] != 0) & (df_f['LON'] != 0)]
            if not coords.empty:
                if len(coords) > 20000:
                    coords = coords.sample(20000)
                    st.caption(f"Amostra de 20.000 pontos (Total: {len(df_f):,})")
                
                fig_map = px.density_mapbox(
                    coords, lat='LAT', lon='LON', radius=5, zoom=3,
                    center=dict(lat=-15.78, lon=-47.92),
                    mapbox_style="carto-positron" if tema['bg_card'] == '#FFFFFF' else "carto-darkmatter"
                )
                fig_map.update_layout(height=600, margin={"r":0,"t":0,"l":0,"b":0})
                st.plotly_chart(fig_map, use_container_width=True)
            else:
                st.warning("Sem coordenadas válidas.")
        else:
            st.info("Colunas LAT/LON não encontradas.")