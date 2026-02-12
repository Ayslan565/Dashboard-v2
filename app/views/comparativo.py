import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from utils import padronizar_grafico

# Mapeamento Completo de Estados (Nome -> Sigla)
MAPA_ESTADOS = {
    # Norte
    'Acre': 'AC', 'Amapá': 'AP', 'Amapa': 'AP', 'Amazonas': 'AM', 'Pará': 'PA', 'Para': 'PA',
    'Rondônia': 'RO', 'Rondonia': 'RO', 'Roraima': 'RR', 'Tocantins': 'TO',
    # Nordeste
    'Alagoas': 'AL', 'Bahia': 'BA', 'Ceará': 'CE', 'Ceara': 'CE', 'Maranhão': 'MA', 'Maranhao': 'MA',
    'Paraíba': 'PB', 'Paraiba': 'PB', 'Pernambuco': 'PE', 'Piauí': 'PI', 'Piaui': 'PI',
    'Rio Grande do Norte': 'RN', 'Sergipe': 'SE',
    # Centro-Oeste
    'Distrito Federal': 'DF', 'Goiás': 'GO', 'Goias': 'GO', 'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS',
    # Sudeste
    'Espírito Santo': 'ES', 'Espirito Santo': 'ES', 'Minas Gerais': 'MG', 
    'Rio de Janeiro': 'RJ', 'São Paulo': 'SP', 'Sao Paulo': 'SP',
    # Sul
    'Paraná': 'PR', 'Parana': 'PR', 'Rio Grande do Sul': 'RS', 'Santa Catarina': 'SC'
}

def render_comparativo(df_prod, df_obitos, tema):
    st.markdown("### ⚖️ Relatório de Eficiência: Entregas vs. Sinistralidade")
    
    # --- 1. TRATAMENTO DE PRODUTOS ---
    if not df_prod.empty and 'UF' in df_prod.columns:
        # Padroniza a UF
        df_prod['UF'] = df_prod['UF'].astype(str).str.upper().str.strip()
        
        # Filtro de UF Válida (Siglas de 2 letras apenas)
        valid_ufs = [
            'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 
            'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
        ]
        df_prod = df_prod[df_prod['UF'].isin(valid_ufs)]
        
        # Agrupa
        df_prod_uf = df_prod.groupby('UF').size().reset_index(name='Qtd_Produtos')
    else:
        df_prod_uf = pd.DataFrame(columns=['UF', 'Qtd_Produtos'])

    # --- 2. TRATAMENTO DE ÓBITOS ---
    if not df_obitos.empty and 'localidade_nome' in df_obitos.columns:
        df_o = df_obitos.copy()

        # Mapeia Nome -> Sigla
        df_o['UF_Map'] = df_o['localidade_nome'].map(MAPA_ESTADOS)
        df_o = df_o.dropna(subset=['UF_Map'])

        # Soma dos Meses
        possiveis_meses = ['janeiro', 'fevereiro', 'marco', 'março', 'abril', 'maio', 'junho', 
                           'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']
        
        colunas_meses = [c for c in possiveis_meses if c in df_o.columns]

        if colunas_meses:
            for col in colunas_meses:
                df_o[col] = pd.to_numeric(df_o[col], errors='coerce').fillna(0)
            df_o['Total_Calculado'] = df_o[colunas_meses].sum(axis=1)
        else:
            st.warning("⚠️ Colunas de meses não encontradas na tabela de óbitos.")
            df_o['Total_Calculado'] = 0

        # Agrupa
        df_obitos_uf = df_o.groupby('UF_Map')['Total_Calculado'].sum().reset_index(name='Qtd_Obitos')
        df_obitos_uf.rename(columns={'UF_Map': 'UF'}, inplace=True)
    else:
        df_obitos_uf = pd.DataFrame(columns=['UF', 'Qtd_Obitos'])

    # --- 3. CRUZAMENTO ---
    df_final = pd.merge(df_prod_uf, df_obitos_uf, on='UF', how='outer').fillna(0)
    
    # Limpeza final de segurança
    df_final = df_final[df_final['UF'].str.len() == 2]
    df_final = df_final.sort_values('UF')

    if df_final.empty or (df_final['Qtd_Produtos'].sum() == 0 and df_final['Qtd_Obitos'].sum() == 0):
        st.warning("Não há dados suficientes para gerar o gráfico.")
        return

    # --- 4. FILTROS ---
    st.divider()
    cols_filt, _ = st.columns([1, 2])
    with cols_filt:
        sel_uf = st.multiselect("Filtrar Estados:", df_final['UF'].unique(), placeholder="Selecione para filtrar...")
    
    if sel_uf:
        df_final = df_final[df_final['UF'].isin(sel_uf)]

    # --- 5. GRÁFICO ---
    fig = go.Figure()

    # Barra: Produtos (Eixo Esquerdo)
    fig.add_trace(go.Bar(
        x=df_final['UF'],
        y=df_final['Qtd_Produtos'],
        name='Produtos Entregues',
        marker_color='#2196F3',
        yaxis='y1',
        opacity=0.75,
        text=df_final['Qtd_Produtos'],
        textposition='auto'
    ))

    # Linha: Óbitos (Eixo Direito) - COM RÓTULOS DE DADOS ATIVADOS
    fig.add_trace(go.Scatter(
        x=df_final['UF'],
        y=df_final['Qtd_Obitos'],
        name='Óbitos (Total 2024)',
        mode='lines+markers+text',  # <--- ADICIONADO '+text'
        text=df_final['Qtd_Obitos'].apply(lambda x: f"{x:,.0f}"), # <--- VALORES FORMATADOS
        textposition="top center",  # <--- POSIÇÃO DO TEXTO
        line=dict(color='#FF5252', width=3),
        marker=dict(size=8, color='#FF5252', symbol='circle'),
        yaxis='y2'
    ))

    # Layout
    fig.update_layout(
        title="<b>Correlação:</b> Volume de Entregas (PNATRANS) x Óbitos no Trânsito",
        xaxis=dict(title="Estado (UF)"),
        yaxis=dict(
            title="Qtd Produtos",
            titlefont=dict(color="#2196F3"),
            tickfont=dict(color="#2196F3"),
            showgrid=False
        ),
        yaxis2=dict(
            title="Qtd Óbitos",
            titlefont=dict(color="#FF5252"),
            tickfont=dict(color="#FF5252"),
            overlaying='y',
            side='right',
            showgrid=True,
            gridcolor=tema['grid_color']
        ),
        legend=dict(x=0.5, y=1.1, orientation="h", xanchor="center"),
        height=600,
        margin=dict(l=50, r=50, t=80, b=50),
        hovermode="x unified"
    )

    st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

    with st.expander("📋 Ver Tabela de Dados Consolidados"):
        st.dataframe(df_final.set_index('UF'), use_container_width=True)