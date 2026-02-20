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

    # Adicionar agregados de População (Regiões e Brasil)
    POP_UF['NORTE'] = sum([POP_UF[e] for e in ['ACRE', 'AMAPÁ', 'AMAZONAS', 'PARÁ', 'RONDÔNIA', 'RORAIMA', 'TOCANTINS']])
    POP_UF['NORDESTE'] = sum([POP_UF[e] for e in ['ALAGOAS', 'BAHIA', 'CEARÁ', 'MARANHÃO', 'PARAÍBA', 'PERNAMBUCO', 'PIAUÍ', 'RIO GRANDE DO NORTE', 'SERGIPE']])
    POP_UF['SUDESTE'] = sum([POP_UF[e] for e in ['ESPÍRITO SANTO', 'MINAS GERAIS', 'RIO DE JANEIRO', 'SÃO PAULO']])
    POP_UF['SUL'] = sum([POP_UF[e] for e in ['PARANÁ', 'RIO GRANDE DO SUL', 'SANTA CATARINA']])
    POP_UF['CENTRO-OESTE'] = sum([POP_UF[e] for e in ['DISTRITO FEDERAL', 'GOIÁS', 'MATO GROSSO', 'MATO GROSSO DO SUL']])
    POP_UF['BRASIL'] = POP_UF['NORTE'] + POP_UF['NORDESTE'] + POP_UF['SUDESTE'] + POP_UF['SUL'] + POP_UF['CENTRO-OESTE']

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

    meses = ['janeiro', 'fevereiro', 'marco', 'abril', 'maio', 'junho', 
             'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']
    colunas_meses = [m for m in meses if m in df.columns]

    # --- 2.5 AGREGAÇÃO HIERÁRQUICA (ESTADOS -> REGIÕES -> BRASIL) ---
    if 'local' in df.columns:
        df['local'] = df['local'].astype(str).str.upper().str.strip()
        
        # Filtra para manter apenas Estados inicialmente (evitar duplicidade de dados originais)
        regioes_macro = ['NORTE', 'NORDESTE', 'SUDESTE', 'SUL', 'CENTRO-OESTE', 'BRASIL']
        df_estados = df[~df['local'].isin(regioes_macro)].copy()
        
        def mapear_regiao(uf):
            if uf in ['ACRE', 'AMAPÁ', 'AMAZONAS', 'PARÁ', 'RONDÔNIA', 'RORAIMA', 'TOCANTINS']: return 'NORTE'
            if uf in ['ALAGOAS', 'BAHIA', 'CEARÁ', 'MARANHÃO', 'PARAÍBA', 'PERNAMBUCO', 'PIAUÍ', 'RIO GRANDE DO NORTE', 'SERGIPE']: return 'NORDESTE'
            if uf in ['ESPÍRITO SANTO', 'MINAS GERAIS', 'RIO DE JANEIRO', 'SÃO PAULO']: return 'SUDESTE'
            if uf in ['PARANÁ', 'RIO GRANDE DO SUL', 'SANTA CATARINA']: return 'SUL'
            if uf in ['DISTRITO FEDERAL', 'GOIÁS', 'MATO GROSSO', 'MATO GROSSO DO SUL']: return 'CENTRO-OESTE'
            return 'OUTROS'
            
        df_estados['regiao'] = df_estados['local'].apply(mapear_regiao)
        
        # Define colunas de valores (meses ou total) e as descritivas (ano, indicador...)
        cols_somar = colunas_meses if colunas_meses else (['total'] if 'total' in df.columns else [])
        cols_agrupar = [c for c in df_estados.columns if c not in cols_somar + ['total_calculado', 'local', 'regiao']]
        
        # Converte as colunas a serem somadas para numérico para evitar erros no groupby
        for c in cols_somar:
            df_estados[c] = pd.to_numeric(df_estados[c], errors='coerce').fillna(0)
            
        # 1. Agrega as Regiões
        df_regioes_agg = df_estados.groupby(cols_agrupar + ['regiao'], dropna=False)[cols_somar].sum().reset_index()
        df_regioes_agg = df_regioes_agg.rename(columns={'regiao': 'local'})
        
        # 2. Agrega o Brasil
        df_brasil_agg = df_estados.groupby(cols_agrupar, dropna=False)[cols_somar].sum().reset_index()
        df_brasil_agg['local'] = 'BRASIL'
        
        # Junta os três níveis no Dataframe Principal
        df_estados = df_estados.drop(columns=['regiao'])
        df = pd.concat([df_estados, df_regioes_agg, df_brasil_agg], ignore_index=True)

    # Cálculo do Total (Soma dos Meses ou Total base)
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
    
    metrica = st.sidebar.radio(
        "📊 Métrica de Exibição:",
        ["Absoluto (Total)", "Por 10.000 Habitantes"],
        help="Altera os gráficos para números absolutos ou taxa proporcional à população (Base 2025)."
    )

    anos = sorted(df['ano'].unique(), reverse=True)
    sel_anos = st.sidebar.multiselect("📅 Ano:", anos, default=anos[:1] if len(anos)>0 else anos)

    inds = []
    if 'indicador' in df.columns:
        inds = sorted([str(i) for i in df['indicador'].unique() if i and str(i) != 'NÃO INFORMADO'])
    sel_ind = st.sidebar.multiselect("🚦 Indicador (Grupo V):", inds)

    # Adicionado Brasil e Regiões de volta na lista de opções para pesquisa direta
    locs = []
    if 'local' in df.columns:
        locs = sorted([l for l in df['local'].unique() if l])
    sel_loc = st.sidebar.multiselect("🗺️ Localidade:", locs)

    df_f = df.copy()
    if sel_anos: df_f = df_f[df_f['ano'].isin(sel_anos)]
    if sel_ind: df_f = df_f[df_f['indicador'].isin(sel_ind)]
    if sel_loc: df_f = df_f[df_f['local'].isin(sel_loc)]

    if df_f.empty:
        st.warning("⚠️ Nenhum dado encontrado com os filtros atuais.")
        return

    # --- 4. PREPARAÇÃO DA BASE VISUAL (Impede duplicação em visões gerais) ---
    regioes_excluir = ['NORTE', 'NORDESTE', 'SUDESTE', 'SUL', 'CENTRO-OESTE', 'BRASIL']
    
    # Se o usuário não filtrou localidades específicas, excluímos as agregações (Região e Brasil) 
    # dos cálculos gerais para não triplicar o resultado nos KPIs
    if not sel_loc and 'local' in df_f.columns:
        df_base_charts = df_f[~df_f['local'].isin(regioes_excluir)]
    else:
        df_base_charts = df_f

    # --- 5. KPIs ---
    total = df_base_charts['total_calculado'].sum()

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
    if 'indicador' in df_base_charts.columns and not df_base_charts.empty:
        try: 
            top_ind = df_base_charts.groupby('indicador')['total_calculado'].sum().idxmax().split(' ')[0]
        except: pass

    k1, k2, k3 = st.columns(3)
    with k1: st.markdown(html_card("Total Óbitos", f"{total:,.0f}".replace(",", "."), "Vidas Perdidas (Absoluto)", tema), unsafe_allow_html=True)
    with k2: st.markdown(html_card(label_ano, texto_ano, "Base: SIM/DATASUS", tema), unsafe_allow_html=True)
    with k3: st.markdown(html_card("Maior Grupo", top_ind, "Indicador Principal", tema), unsafe_allow_html=True)

    st.divider()

    # --- LÓGICA DE PLOTAGEM ---
    col_valor = 'total_calculado'
    sufixo_tooltip = " Óbitos"
    usar_taxa = metrica == "Por 100.000 Habitantes"
    
    if usar_taxa:
        st.info("ℹ️ Exibindo dados normalizados por população (Fonte: Projeção 2025).")
        sufixo_tooltip = " mortes/100k hab"

    # --- 6. ABAS VISUAIS ---
    tabs = st.tabs(["📍 Geografia (Regiões vs Estados)", "📊 Evolução", "🚦 Indicadores"])

    # ABA 1: GEOGRAFIA
    with tabs[0]:
        st.subheader("Distribuição Geográfica")
        c1, c2 = st.columns([1, 2])

        if 'local' in df_f.columns:
            # Puxa diretamente os agregados da tabela (sem precisar somar estados novamente)
            lista_regioes = ['NORTE', 'NORDESTE', 'SUDESTE', 'SUL', 'CENTRO-OESTE']
            df_regioes = df_f[df_f['local'].isin(lista_regioes)].groupby('local')['total_calculado'].sum().reset_index()
            
            # Estados para o Gráfico de Barras
            df_estados = df_f[~df_f['local'].isin(regioes_excluir)].groupby('local')['total_calculado'].sum().reset_index()
            
            # CÁLCULO DA TAXA
            if usar_taxa:
                df_estados['pop'] = df_estados['local'].map(POP_UF)
                df_estados['taxa'] = (df_estados['total_calculado'] / df_estados['pop']) * 100000
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
                else: 
                    st.info("Regiões não aplicáveis ao filtro atual.")

            with c2:
                titulo_ranking = "**Ranking de Estados (Por 10.000 Hab)**" if usar_taxa else "**Ranking de Estados (Absoluto)**"
                st.markdown(titulo_ranking)
                
                if not df_estados.empty:
                    altura = max(600, len(df_estados) * 35)
                    text_fmt = '.2f' if usar_taxa else '.0f'
                    
                    fig_bar = px.bar(df_estados.head(27), x=col_plot_est, y='local', orientation='h', 
                                     text=col_plot_est, color=col_plot_est, color_continuous_scale='Blues')
                    
                    fig_bar.update_traces(textposition='outside', texttemplate='%{text:' + text_fmt + '}')
                    fig_bar.update_layout(yaxis=dict(autorange="reversed"), xaxis_title=f"Valor ({sufixo_tooltip})", height=altura, margin=dict(r=100))
                    st.plotly_chart(padronizar_grafico(fig_bar, tema), use_container_width=True)
                else: 
                    st.info("Sem dados de Estados para exibir (verifique se filtrou por Brasil/Região).")

    # ABA 2: TEMPORAL
    with tabs[1]:
        st.subheader("Evolução Temporal")
        
        meses_ok = [m for m in meses if m in df_base_charts.columns]
        if meses_ok:
            df_melt = df_base_charts.melt(id_vars=['ano'], value_vars=meses_ok, var_name='Mes', value_name='Qtd')
            df_line = df_melt.groupby(['ano', 'Mes'])['Qtd'].sum().reset_index()
            
            if usar_taxa:
                st.caption("*O gráfico temporal é mantido em números absolutos para visualização da sazonalidade.*")

            map_mes = {m: i for i, m in enumerate(meses)}
            df_line['ordem'] = df_line['Mes'].map(map_mes)
            df_line = df_line.sort_values(['ano', 'ordem'])
            
            fig_line = px.line(df_line, x='Mes', y='Qtd', color='ano', markers=True, text='Qtd')
            fig_line.update_traces(textposition="top center")
            st.plotly_chart(padronizar_grafico(fig_line, tema), use_container_width=True)
        else:
            st.warning("Colunas de meses não encontradas no dataset.")

    # ABA 3: INDICADORES
    with tabs[2]:
        st.subheader("Ranking por Tipo de Vítima")
        if 'indicador' in df_base_charts.columns:
            df_ind = df_base_charts.groupby('indicador')['total_calculado'].sum().sort_values(ascending=False).head(15).reset_index()
            
            if not df_ind.empty:
                fig_ind = px.bar(df_ind, x='total_calculado', y='indicador', orientation='h', 
                                 text='total_calculado', color='total_calculado', color_continuous_scale='Reds')
                fig_ind.update_traces(textposition='outside')
                fig_ind.update_layout(yaxis=dict(autorange="reversed"), xaxis_title="Óbitos (Absoluto)", height=600, margin=dict(r=100))
                st.plotly_chart(padronizar_grafico(fig_ind, tema), use_container_width=True)
            else:
                st.info("Sem indicadores para exibir no filtro atual.")

    with st.expander("📋 Ver Dados Brutos (Contém Estados, Regiões e Brasil)"):
        st.dataframe(df_f.head(100), use_container_width=True)
        st.download_button("📥 Baixar Todos os Dados (CSV)", converter_csv(df_f), "obitos_datasus_agregado.csv")    