import streamlit as st
import plotly.express as px
import pandas as pd
from utils import html_card, padronizar_grafico, converter_csv

def render_obitos(df, tema):
    st.markdown("### 🏥 Óbitos no Trânsito (Fonte: SIM/DATASUS)")
    
    if df.empty:
        st.warning("⚠️ Tabela vazia. Verifique se o ETL rodou corretamente.")
        return

    # --- 1. DADOS POPULACIONAIS (PROJEÇÃO 2025) ---
    # Atualizado conforme dados fornecidos
    POP_UF = {
        # Norte
        'RONDÔNIA': 1751950, 'RONDONIA': 1751950, 'RO': 1751950,
        'ACRE': 884372, 'AC': 884372,
        'AMAZONAS': 4321616, 'AM': 4321616,
        'RORAIMA': 738772, 'RR': 738772,
        'PARÁ': 8711196, 'PARA': 8711196, 'PA': 8711196,
        'AMAPÁ': 806517, 'AMAPA': 806517, 'AP': 806517,
        'TOCANTINS': 1586859, 'TO': 1586859,
        
        # Nordeste
        'MARANHÃO': 7018211, 'MARANHAO': 7018211, 'MA': 7018211,
        'PIAUÍ': 3384547, 'PIAUI': 3384547, 'PI': 3384547,
        'CEARÁ': 9268836, 'CEARA': 9268836, 'CE': 9268836,
        'RIO GRANDE DO NORTE': 3455236, 'RN': 3455236,
        'PARAÍBA': 4164468, 'PARAIBA': 4164468, 'PB': 4164468,
        'PERNAMBUCO': 9562007, 'PE': 9562007,
        'ALAGOAS': 3220848, 'AL': 3220848,
        'SERGIPE': 2299425, 'SE': 2299425,
        'BAHIA': 14870907, 'BA': 14870907,
        
        # Sudeste
        'MINAS GERAIS': 21393441, 'MG': 21393441,
        'ESPÍRITO SANTO': 4126854, 'ESPIRITO SANTO': 4126854, 'ES': 4126854,
        'RIO DE JANEIRO': 17223547, 'RJ': 17223547,
        'SÃO PAULO': 46081801, 'SAO PAULO': 46081801, 'SP': 46081801,
        
        # Sul
        'PARANÁ': 11890517, 'PARANA': 11890517, 'PR': 11890517,
        'SANTA CATARINA': 8187029, 'SC': 8187029,
        'RIO GRANDE DO SUL': 11233263, 'RS': 11233263,
        
        # Centro-Oeste
        'MATO GROSSO DO SUL': 2924631, 'MS': 2924631,
        'MATO GROSSO': 3893659, 'MT': 3893659,
        'GOIÁS': 7423629, 'GOIAS': 7423629, 'GO': 7423629,
        'DISTRITO FEDERAL': 2996899, 'DF': 2996899
    }

    # --- 2. PADRONIZAÇÃO DE COLUNAS ---
    df.columns = [c.lower().strip() for c in df.columns]
    
    mapa_ideal = {
        'ano': ['ano', 'ano_nome', 'ano_obito', 'data_ano'],
        'local': ['localidade_nome', 'local_nome', 'localidade', 'uf', 'estado', 'municipio'],
        'indicador': ['indicador_nome', 'indicador', 'causa', 'tipo_acidente', 'grupo_cid'],
        'categoria': ['categoria_nome', 'categoria'],
        'sexo': ['sexo_nome', 'sexo', 'genero'],
        'raca': ['racacor_nome', 'racacor', 'raca', 'cor', 'raca_cor'],
        'etaria': ['grupoetario_nome', 'grupoetario', 'faixa_etaria', 'idade_grupo', 'grupo_etario']
    }
    
    for novo, lista in mapa_ideal.items():
        if novo not in df.columns:
            for velho in lista:
                if velho in df.columns:
                    df = df.rename(columns={velho: novo})
                    break

    # Garante coluna de Ano numérica
    if 'ano' not in df.columns: df['ano'] = 2024
    df['ano'] = pd.to_numeric(df['ano'], errors='coerce').fillna(2024).astype(int)

    # Cálculo do Total (Soma dos Meses)
    meses = ['janeiro', 'fevereiro', 'marco', 'abril', 'maio', 'junho', 
             'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']
    
    colunas_meses = [m for m in meses if m in df.columns]
    
    if colunas_meses:
        for m in colunas_meses: 
            df[m] = pd.to_numeric(df[m], errors='coerce').fillna(0)
        df['total_calculado'] = df[colunas_meses].sum(axis=1)
    elif 'total' in df.columns:
        df['total_calculado'] = pd.to_numeric(df['total'], errors='coerce').fillna(0)
    else:
        df['total_calculado'] = 0

    # --- 3. FILTROS ---
    st.sidebar.divider()
    st.sidebar.subheader("🔍 Filtros Avançados")
    
    # SELETOR DE MÉTRICA
    metrica = st.sidebar.radio(
        "📊 Métrica de Exibição:",
        ["Absoluto (Total)", "Por 1.000 Habitantes"],
        help="Altera os gráficos para números absolutos ou taxa proporcional à população (Base 2025)."
    )

    anos = sorted(df['ano'].unique(), reverse=True)
    sel_anos = st.sidebar.multiselect("📅 Ano:", anos, default=anos[:1] if len(anos)>0 else anos)

    inds = []
    if 'indicador' in df.columns:
        inds = sorted([str(i) for i in df['indicador'].unique() if i and str(i) != 'NÃO INFORMADO'])
    sel_ind = st.sidebar.multiselect("🚦 Indicador (Grupo V):", inds)

    termos_macro = ['BRASIL', 'NORTE', 'NORDESTE', 'SUDESTE', 'SUL', 'CENTRO-OESTE']
    locs = []
    if 'local' in df.columns:
        locs = sorted([l for l in df['local'].unique() if l and str(l).upper() not in termos_macro])
    sel_loc = st.sidebar.multiselect("🗺️ Estado/Região:", locs)

    df_f = df.copy()
    if sel_anos: df_f = df_f[df_f['ano'].isin(sel_anos)]
    if sel_ind: df_f = df_f[df_f['indicador'].isin(sel_ind)]
    if sel_loc: df_f = df_f[df_f['local'].isin(sel_loc)]

    if df_f.empty:
        st.warning("⚠️ Nenhum dado encontrado com os filtros atuais.")
        return

    # --- 4. KPIs ---
    regioes_excluir = [
        'Norte', 'Nordeste', 'Sudeste', 'Sul', 'Centro-Oeste', 'Brasil',
        'NORTE', 'NORDESTE', 'SUDESTE', 'SUL', 'CENTRO-OESTE', 'BRASIL'
    ]

    # Total Absoluto
    if not sel_loc and 'local' in df_f.columns:
        total = df_f[~df_f['local'].isin(regioes_excluir)]['total_calculado'].sum()
    else:
        total = df_f['total_calculado'].sum()

    if len(sel_anos) == 1:
        texto_ano = str(sel_anos[0])
        label_ano = "Ano Selecionado"
    elif len(sel_anos) > 1:
        texto_ano = f"{min(sel_anos)} a {max(sel_anos)}"
        label_ano = "Período"
    else:
        texto_ano = "Todos"
        label_ano = "Série Histórica"

    top_ind = "-"
    if 'indicador' in df_f.columns and not df_f.empty:
        try: 
            df_causa = df_f[~df_f['local'].isin(regioes_excluir)] if 'local' in df_f.columns else df_f
            top_ind = df_causa.groupby('indicador')['total_calculado'].sum().idxmax().split(' ')[0]
        except: pass

    # KPIs Renderizados
    k1, k2, k3 = st.columns(3)
    with k1: st.markdown(html_card("Total Óbitos", f"{total:,.0f}".replace(",", "."), "Vidas Perdidas (Absoluto)", tema), unsafe_allow_html=True)
    with k2: st.markdown(html_card(label_ano, texto_ano, "Base: SIM/DATASUS", tema), unsafe_allow_html=True)
    with k3: st.markdown(html_card("Maior Grupo", top_ind, "Indicador Principal", tema), unsafe_allow_html=True)

    st.divider()

    # --- LÓGICA DE PLOTAGEM ---
    col_valor = 'total_calculado'
    sufixo_tooltip = " Óbitos"
    usar_taxa = metrica == "Por 1.000 Habitantes"
    
    if usar_taxa:
        st.info("ℹ️ Exibindo dados normalizados por população (Fonte: Projeção 2025).")
        sufixo_tooltip = " mortes/1k hab"

    # --- 5. ABAS VISUAIS ---
    tabs = st.tabs(["📍 Geografia (Regiões vs Estados)", "📊 Evolução", "🚦 Indicadores"])

    # ABA 1: GEOGRAFIA
    with tabs[0]:
        st.subheader("Distribuição Geográfica")
        c1, c2 = st.columns([1, 2])

        if 'local' in df_f.columns:
            lista_regioes = ['Norte', 'Nordeste', 'Sudeste', 'Sul', 'Centro-Oeste', 'NORTE', 'NORDESTE', 'SUDESTE', 'SUL', 'CENTRO-OESTE']
            
            # Agregação Regiões
            df_regioes = df_f[df_f['local'].isin(lista_regioes)].groupby('local')['total_calculado'].sum().reset_index()
            
            # Agregação Estados
            df_estados = df_f[~df_f['local'].isin(regioes_excluir)].groupby('local')['total_calculado'].sum().reset_index()
            
            # CÁLCULO DA TAXA
            if usar_taxa:
                df_estados['pop'] = df_estados['local'].str.upper().map(POP_UF)
                # Cálculo da taxa por 1.000 habitantes
                df_estados['taxa'] = (df_estados['total_calculado'] / df_estados['pop']) * 1000
                df_estados = df_estados.dropna(subset=['taxa'])
                col_plot_est = 'taxa'
            else:
                col_plot_est = 'total_calculado'

            df_estados = df_estados.sort_values(col_plot_est, ascending=False)

            with c1:
                st.markdown("**Por Região (Absoluto)**")
                if not df_regioes.empty:
                    fig_pizza = px.pie(df_regioes, values='total_calculado', names='local', hole=0.4,
                                       color_discrete_sequence=px.colors.qualitative.Bold)
                    fig_pizza.update_traces(textposition='inside', textinfo='percent+label')
                    fig_pizza.update_layout(showlegend=False)
                    st.plotly_chart(padronizar_grafico(fig_pizza, tema), use_container_width=True)
                else: st.info("Sem dados de Região.")

            with c2:
                titulo_ranking = "**Ranking de Estados (Por 1.000 Hab)**" if usar_taxa else "**Ranking de Estados (Absoluto)**"
                st.markdown(titulo_ranking)
                
                if not df_estados.empty:
                    altura = max(600, len(df_estados) * 35)
                    text_fmt = '.2f' if usar_taxa else '.0f'
                    
                    fig_bar = px.bar(df_estados.head(27), x=col_plot_est, y='local', orientation='h', 
                                     text=col_plot_est, color=col_plot_est, color_continuous_scale='Blues')
                    
                    fig_bar.update_traces(textposition='outside', texttemplate='%{text:' + text_fmt + '}')
                    fig_bar.update_layout(yaxis=dict(autorange="reversed"), xaxis_title=f"Valor ({sufixo_tooltip})", height=altura, margin=dict(r=100))
                    st.plotly_chart(padronizar_grafico(fig_bar, tema), use_container_width=True)
                else: st.info("Sem dados de Estados.")

    # ABA 2: TEMPORAL
    with tabs[1]:
        st.subheader("Evolução Temporal")
        df_temp = df_f[~df_f['local'].isin(regioes_excluir)] if 'local' in df_f.columns else df_f
        
        meses_ok = [m for m in meses if m in df_temp.columns]
        if meses_ok:
            df_melt = df_temp.melt(id_vars=['ano'], value_vars=meses_ok, var_name='Mes', value_name='Qtd')
            df_line = df_melt.groupby(['ano', 'Mes'])['Qtd'].sum().reset_index()
            
            if usar_taxa:
                st.caption("*O gráfico temporal é mantido em números absolutos para visualização de sazonalidade mensal.*")

            map_mes = {m: i for i, m in enumerate(meses)}
            df_line['ordem'] = df_line['Mes'].map(map_mes)
            df_line = df_line.sort_values(['ano', 'ordem'])
            
            fig_line = px.line(df_line, x='Mes', y='Qtd', color='ano', markers=True, text='Qtd')
            fig_line.update_traces(textposition="top center")
            st.plotly_chart(padronizar_grafico(fig_line, tema), use_container_width=True)
        else:
            st.warning("Colunas de meses não encontradas.")

    # ABA 3: INDICADORES
    with tabs[2]:
        st.subheader("Ranking por Tipo de Vítima")
        if 'indicador' in df_f.columns:
            df_ind_base = df_f[~df_f['local'].isin(regioes_excluir)] if 'local' in df_f.columns else df_f
            
            df_ind = df_ind_base.groupby('indicador')['total_calculado'].sum().sort_values(ascending=False).head(15).reset_index()
            
            fig_ind = px.bar(df_ind, x='total_calculado', y='indicador', orientation='h', 
                             text='total_calculado', color='total_calculado', color_continuous_scale='Reds')
            fig_ind.update_traces(textposition='outside')
            fig_ind.update_layout(yaxis=dict(autorange="reversed"), xaxis_title="Óbitos (Absoluto)", height=600, margin=dict(r=100))
            st.plotly_chart(padronizar_grafico(fig_ind, tema), use_container_width=True)

    with st.expander("📋 Ver Dados Brutos"):
        st.dataframe(df_f.head(100), use_container_width=True)
        st.download_button("📥 Baixar Dados (CSV)", converter_csv(df_f), "obitos_datasus.csv")