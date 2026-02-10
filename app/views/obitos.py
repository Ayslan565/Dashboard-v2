import streamlit as st
import plotly.express as px
import pandas as pd
from utils import html_card, padronizar_grafico

def render_obitos(df, tema):
    st.markdown("### 🏥 Óbitos no Trânsito (Fonte: SIM/DATASUS)")
    
    if df.empty:
        st.warning("⚠️ Tabela vazia. Verifique se o ETL rodou corretamente.")
        return

    # --- 1. PADRONIZAÇÃO ---
    mapa_ideal = {
        'ano': ['ano', 'ano_nome'],
        'local': ['localidade_nome', 'local_nome'],
        'indicador': ['indicador_nome', 'indicador'],
        'categoria': ['categoria_nome', 'categoria'],
        'sexo': ['sexo_nome', 'sexo'],
        'raca': ['racacor_nome', 'racacor'],
        'etaria': ['grupoetario_nome', 'grupoetario']
    }
    for novo, lista in mapa_ideal.items():
        if novo not in df.columns:
            for velho in lista:
                if velho in df.columns:
                    df = df.rename(columns={velho: novo})
                    break

    if 'ano' not in df.columns: df['ano'] = 2024
    df['ano'] = pd.to_numeric(df['ano'], errors='coerce').fillna(2024).astype(int)

    meses = ['janeiro', 'fevereiro', 'marco', 'abril', 'maio', 'junho', 
             'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']
    for m in meses: 
        if m in df.columns: df[m] = pd.to_numeric(df[m], errors='coerce').fillna(0)
    df['total_calculado'] = df[[m for m in meses if m in df.columns]].sum(axis=1)

    # --- 2. FILTROS ---
    st.sidebar.divider()
    st.sidebar.subheader("🔍 Filtros Avançados")

    anos = sorted(df['ano'].unique(), reverse=True)
    sel_anos = st.sidebar.multiselect("📅 Ano:", anos, default=anos[:2] if len(anos)>1 else anos)

    inds = []
    if 'indicador' in df.columns:
        inds = sorted([str(i) for i in df['indicador'].unique() if i and str(i) != 'NÃO INFORMADO'])
    sel_ind = st.sidebar.multiselect("🚦 Indicador (Grupo V):", inds)

    locs = sorted([l for l in df['local'].unique() if l]) if 'local' in df.columns else []
    sel_loc = st.sidebar.multiselect("🗺️ Estado/Região:", locs)

    df_f = df.copy()
    if sel_anos: df_f = df_f[df_f['ano'].isin(sel_anos)]
    if sel_ind: df_f = df_f[df_f['indicador'].isin(sel_ind)]
    if sel_loc: df_f = df_f[df_f['local'].isin(sel_loc)]

    if df_f.empty:
        st.warning("⚠️ Nenhum dado encontrado.")
        return

    # --- 3. KPIs ---
    regioes = ['Norte', 'Nordeste', 'Sudeste', 'Sul', 'Centro-Oeste', 'Brasil']
    if not sel_loc and 'local' in df_f.columns:
        total = df_f[~df_f['local'].isin(regioes)]['total_calculado'].sum()
    else:
        total = df_f['total_calculado'].sum()

    k1, k2, k3 = st.columns(3)
    with k1: st.markdown(html_card("Total Óbitos", f"{total:,.0f}", "Soma", tema), unsafe_allow_html=True)
    with k2: st.markdown(html_card("Anos", f"{len(sel_anos)}", "Selecionados", tema), unsafe_allow_html=True)
    
    top_ind = "-"
    if 'indicador' in df_f.columns and not df_f.empty:
        try: top_ind = df_f.groupby('indicador')['total_calculado'].sum().idxmax().split(' ')[0]
        except: pass
    with k3: st.markdown(html_card("Maior Grupo", top_ind, "Indicador", tema), unsafe_allow_html=True)

    st.divider()

    # --- 4. ABAS ---
    tabs = st.tabs(["📍 Geografia (Regiões vs Estados)", "📊 Evolução", "🚦 Indicadores", "👥 Perfil"])

    # ABA 1: GEOGRAFIA (SEPARADA)
    with tabs[0]:
        st.subheader("Distribuição Geográfica")
        c1, c2 = st.columns([1, 2])

        if 'local' in df_f.columns:
            df_regioes = df_f[df_f['local'].isin(regioes[:5])].groupby('local')['total_calculado'].sum().reset_index()
            df_estados = df_f[~df_f['local'].isin(regioes)].groupby('local')['total_calculado'].sum().sort_values(ascending=False).reset_index()

            with c1:
                st.markdown("**Por Região**")
                if not df_regioes.empty:
                    fig_pizza = px.pie(df_regioes, values='total_calculado', names='local', hole=0.4,
                                       color_discrete_sequence=px.colors.qualitative.Bold)
                    fig_pizza.update_traces(textposition='inside', textinfo='percent+label')
                    fig_pizza.update_layout(showlegend=False)
                    st.plotly_chart(padronizar_grafico(fig_pizza, tema), use_container_width=True)
                else: st.info("Sem dados de Região.")

            with c2:
                st.markdown("**Ranking de Estados (UF)**")
                if not df_estados.empty:
                    # Ajuste de Altura Dinâmico: 30px por barra + margem (mínimo 600)
                    altura_grafico = max(600, len(df_estados) * 35)
                    
                    fig_bar = px.bar(df_estados.head(30), x='total_calculado', y='local', orientation='h', 
                                     text='total_calculado', color='total_calculado', color_continuous_scale='Blues')
                    
                    # Rótulos de dados FORA da barra e maiores
                    fig_bar.update_traces(textposition='outside', textfont_size=12)
                    
                    # Aumenta o tamanho e inverte eixo Y
                    fig_bar.update_layout(
                        yaxis=dict(autorange="reversed"), 
                        xaxis_title="Óbitos", 
                        height=altura_grafico, # Altura aumentada aqui
                        margin=dict(r=100) # Margem direita para caber o número
                    )
                    st.plotly_chart(padronizar_grafico(fig_bar, tema), use_container_width=True)
                else: st.info("Sem dados de Estados.")

    # ABA 2: TEMPORAL
    with tabs[1]:
        st.subheader("Sazonalidade Mensal")
        df_temp = df_f[~df_f['local'].isin(['Brasil'])] if 'local' in df_f.columns else df_f
        
        meses_ok = [m for m in meses if m in df_temp.columns]
        if meses_ok:
            df_melt = df_temp.melt(id_vars=['ano'], value_vars=meses_ok, var_name='Mes', value_name='Qtd')
            df_line = df_melt.groupby(['ano', 'Mes'])['Qtd'].sum().reset_index()
            
            map_mes = {m: i for i, m in enumerate(meses)}
            df_line['ordem'] = df_line['Mes'].map(map_mes)
            df_line = df_line.sort_values(['ano', 'ordem'])
            
            fig_line = px.line(df_line, x='Mes', y='Qtd', color='ano', markers=True, text='Qtd')
            fig_line.update_traces(textposition="top center") # Rótulos na linha
            st.plotly_chart(padronizar_grafico(fig_line, tema), use_container_width=True)

    # ABA 3: INDICADORES
    with tabs[2]:
        st.subheader("Ranking por Tipo de Vítima")
        if 'indicador' in df_f.columns:
            df_ind = df_f[~df_f['local'].isin(['Brasil'])].groupby('indicador')['total_calculado'].sum().sort_values(ascending=False).head(15).reset_index()
            
            fig_ind = px.bar(df_ind, x='total_calculado', y='indicador', orientation='h', 
                             text='total_calculado', color='total_calculado', color_continuous_scale='Reds')
            
            fig_ind.update_traces(textposition='outside')
            fig_ind.update_layout(yaxis=dict(autorange="reversed"), height=600, margin=dict(r=100))
            st.plotly_chart(padronizar_grafico(fig_ind, tema), use_container_width=True)

    # ABA 4: PERFIL
    with tabs[3]:
        c1, c2 = st.columns(2)
        df_perf = df_f[~df_f['local'].isin(['Brasil'])]
        
        with c1:
            st.subheader("Faixa Etária")
            if 'faixa_etaria' in df_perf.columns:
                df_age = df_perf.groupby('faixa_etaria')['total_calculado'].sum().reset_index()
                fig = px.bar(df_age, x='total_calculado', y='faixa_etaria', orientation='h', text='total_calculado')
                fig.update_traces(textposition='outside')
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
        with c2:
            st.subheader("Raça / Cor")
            if 'raca_cor' in df_perf.columns:
                df_raca = df_perf.groupby('raca_cor')['total_calculado'].sum().reset_index()
                fig = px.pie(df_raca, values='total_calculado', names='raca_cor')
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

    with st.expander("📋 Ver Dados Brutos"):
        st.dataframe(df_f.head(100), use_container_width=True)