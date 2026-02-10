import streamlit as st
import plotly.express as px
import pandas as pd
from utils import html_card, padronizar_grafico, converter_csv

def render_prf(df, tema):
    # Título atualizado
    st.markdown("### 🚗 PRF - Monitoramento Avançado de Sinistros")
    
    if df.empty: 
        st.error("⚠️ Base de dados vazia. Verifique a conexão.")
        return

    # --- BARRA LATERAL: FILTROS ---
    st.sidebar.divider()
    st.sidebar.subheader("Filtros")
    
    # Filtro 1: Ano
    anos = sorted(df['ANO'].unique(), reverse=True)
    sel_anos = st.sidebar.multiselect("📅 Ano:", anos, default=[anos[0]] if anos else [])
    
    # Filtro 2: UF
    ufs = sorted(df['UF'].astype(str).unique())
    sel_ufs = st.sidebar.multiselect("🗺️ Estado (UF):", ufs)

    # Filtro 3: BR (Rodovia)
    df_temp = df
    if sel_anos: df_temp = df_temp[df_temp['ANO'].isin(sel_anos)]
    if sel_ufs: df_temp = df_temp[df_temp['UF'].isin(sel_ufs)]
    
    brs_disponiveis = sorted(df_temp['BR'].astype(str).unique())
    # Limita a lista se for muito grande para não travar
    if len(brs_disponiveis) > 200: brs_disponiveis = brs_disponiveis[:200]
    
    sel_brs = st.sidebar.multiselect("🛣️ Rodovia (BR):", brs_disponiveis)

    # Aplicação Final dos Filtros
    df_f = df.copy()
    if sel_anos: df_f = df_f[df_f['ANO'].isin(sel_anos)]
    if sel_ufs: df_f = df_f[df_f['UF'].isin(sel_ufs)]
    if sel_brs: df_f = df_f[df_f['BR'].astype(str).isin(sel_brs)]

    # --- KPIs GERAIS ---
    k1, k2, k3, k4 = st.columns(4)
    
    # Usando PESID para contar pessoas e ID para sinistros
    total_pessoas = len(df_f) 
    total_sinistros = df_f['ID'].nunique() if 'ID' in df_f.columns else len(df_f)
    
    # Somas de gravidade
    mortos = int(df_f['MORTOS'].sum())
    feridos_graves = int(df_f['FERIDOS_GRAVES'].sum())
    feridos_leves = int(df_f['FERIDOS_LEVES'].sum())
    ilesos = int(df_f['ILESOS'].sum())
    
    # KPIs atualizados com termo "Sinistros"
    with k1: st.markdown(html_card("Sinistros", f"{total_sinistros:,}", "Ocorrências Únicas", tema), unsafe_allow_html=True)
    with k2: st.markdown(html_card("Envolvidos", f"{total_pessoas:,}", "Total de Pessoas", tema), unsafe_allow_html=True)
    with k3: st.markdown(html_card("Óbitos", f"{mortos:,}", "Vítimas Fatais", tema), unsafe_allow_html=True)
    
    # Severidade (Mortos por 100 sinistros)
    sev = (mortos / total_sinistros * 100) if total_sinistros > 0 else 0
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
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

        st.subheader("Distribuição por Idade")
        if 'IDADE' in df_f.columns:
            df_i = df_f[(df_f['IDADE'] > 0) & (df_f['IDADE'] < 110)]
            if not df_i.empty:
                fig = px.histogram(df_i, x="IDADE", nbins=50, color_discrete_sequence=['#2196F3'])
                fig.update_layout(bargap=0.1)
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

    # ABA 2: VEÍCULOS & MARCAS
    with tabs[1]:
        c_veic, c_marca = st.columns(2)
        with c_veic:
            st.subheader("Tipo de Veículo")
            if 'TIPO_VEICULO' in df_f.columns:
                top_v = df_f['TIPO_VEICULO'].value_counts().head(10).reset_index()
                top_v.columns = ['Veículo', 'Qtd']
                fig = px.bar(top_v, x='Qtd', y='Veículo', orientation='h', text_auto=True, color='Qtd')
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
                st.download_button("📥 Baixar", converter_csv(top_v), "veiculos.csv")

        with c_marca:
            st.subheader("Top Marcas/Modelos")
            if 'MARCA' in df_f.columns:
                # Filtra marcas genéricas
                df_m = df_f[~df_f['MARCA'].isin(['NÃO INFORMADO', 'OUTRA', 'OUTRAS', 'NI/NI'])]
                top_m = df_m['MARCA'].value_counts().head(15).reset_index()
                top_m.columns = ['Marca', 'Qtd']
                fig = px.bar(top_m, x='Qtd', y='Marca', orientation='h', text_auto=True, color='Qtd')
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

        # Ano de Fabricação
        if 'ANO_FABRICACAO_VEICULO' in df_f.columns:
            st.subheader("Idade da Frota (Ano Fabricação)")
            df_ano = df_f[(df_f['ANO_FABRICACAO_VEICULO'] > 1980) & (df_f['ANO_FABRICACAO_VEICULO'] <= 2026)]
            if not df_ano.empty:
                fig_a = px.histogram(df_ano, x="ANO_FABRICACAO_VEICULO", nbins=30)
                st.plotly_chart(padronizar_grafico(fig_a, tema), use_container_width=True)

    # ABA 3: LOCALIZAÇÃO
    with tabs[2]:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Por Estado")
            top_uf = df_f.groupby('UF')['ID'].nunique().sort_values(ascending=False).head(15).reset_index()
            # Atualizado nome da coluna e eixo
            top_uf.columns = ['UF', 'Sinistros']
            fig = px.bar(top_uf, x='Sinistros', y='UF', orientation='h', text_auto=True)
            st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        
        with c2:
            st.subheader("Por Município")
            top_mun = df_f.groupby('MUNICIPIO')['ID'].nunique().sort_values(ascending=False).head(15).reset_index()
            # Atualizado nome da coluna e eixo
            top_mun.columns = ['Município', 'Sinistros']
            fig = px.bar(top_mun, x='Sinistros', y='Município', orientation='h', text_auto=True)
            st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

    # ABA 4: CAUSAS & CONTEXTO
    with tabs[3]:
        c_causa, c_clima = st.columns(2)
        with c_causa:
            st.subheader("Causa Principal")
            if 'CAUSA_PRINCIPAL' in df_f.columns:
                top_c = df_f['CAUSA_PRINCIPAL'].value_counts().head(10).reset_index()
                top_c.columns = ['Causa', 'Qtd']
                fig = px.bar(top_c, x='Qtd', y='Causa', orientation='h', text_auto=True, color='Qtd')
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

        with c_clima:
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
                fig = px.bar(df_f['TIPO_PISTA'].value_counts().reset_index(), x='count', y='TIPO_PISTA', orientation='h')
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

    # ABA 5: MAPAfasfasfas
    with tabs[4]:
        st.subheader("Mapa de Calor (Densidade)")
        if 'LAT' in df_f.columns and 'LON' in df_f.columns:
            coords = df_f[(df_f['LAT'] != 0) & (df_f['LON'] != 0)]
            if not coords.empty:
                # Amostragem para performance
                if len(coords) > 20000:
                    coords = coords.sample(20000)
                    st.caption(f"Amostra de 20.000 pontos (Total real: {len(df_f):,})")
                
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