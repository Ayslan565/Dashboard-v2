import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from utils import padronizar_grafico

# Dicionário para converter nomes de estados em siglas
MAPA_ESTADOS = {
    'ACRE': 'AC', 'ALAGOAS': 'AL', 'AMAPA': 'AP', 'AMAPÁ': 'AP', 'AMAZONAS': 'AM',
    'BAHIA': 'BA', 'CEARA': 'CE', 'CEARÁ': 'CE', 'DISTRITO FEDERAL': 'DF',
    'ESPIRITO SANTO': 'ES', 'ESPÍRITO SANTO': 'ES', 'GOIAS': 'GO', 'GOIÁS': 'GO',
    'MARANHAO': 'MA', 'MARANHÃO': 'MA', 'MATO GROSSO': 'MT', 'MATO GROSSO DO SUL': 'MS',
    'MINAS GERAIS': 'MG', 'PARA': 'PA', 'PARÁ': 'PA', 'PARAIBA': 'PB', 'PARAÍBA': 'PB',
    'PARANA': 'PR', 'PARANÁ': 'PR', 'PERNAMBUCO': 'PE', 'PIAUI': 'PI', 'PIAUÍ': 'PI',
    'RIO DE JANEIRO': 'RJ', 'RIO GRANDE DO NORTE': 'RN', 'RIO GRANDE DO SUL': 'RS',
    'RONDONIA': 'RO', 'RONDÔNIA': 'RO', 'RORAIMA': 'RR', 'SANTA CATARINA': 'SC',
    'SAO PAULO': 'SP', 'SÃO PAULO': 'SP', 'SERGIPE': 'SE', 'TOCANTINS': 'TO'
}

def normalizar_uf(valor):
    """Converte nome ou sigla para Sigla (ex: 'São Paulo' -> 'SP')"""
    if pd.isna(valor): return None
    v = str(valor).upper().strip()
    if len(v) == 2: return v
    return MAPA_ESTADOS.get(v, v)

def render_comparativo(df_prod_raw, df_obitos, tema):
    st.markdown("### ⚖️ Correlação: Entregas vs. Sinistralidade")
    
    # --- 1. VALIDAÇÃO DOS PRODUTOS ---
    if df_prod_raw is None or df_prod_raw.empty:
        st.warning("⚠️ Tabela de Produtos está vazia.")
        return

    # Prepara DF Produtos
    df_p = df_prod_raw.copy()
    df_p.columns = [str(c).upper().strip() for c in df_p.columns]

    # Prepara DF Óbitos
    df_o = df_obitos.copy() if not df_obitos.empty else pd.DataFrame()
    if not df_o.empty:
        df_o.columns = [c.lower().strip() for c in df_o.columns]

    # --- 2. FILTRO DE ESTADO (OPCIONAL) ---
    col_uf_prod = next((c for c in ['UF', 'ESTADO', 'SG_UF'] if c in df_p.columns), None)
    col_uf_obito = next((c for c in ['local_nome', 'uf', 'estado', 'nome'] if c in df_o.columns), None)

    opcoes_uf = set()
    if col_uf_prod: opcoes_uf.update(df_p[col_uf_prod].dropna().apply(normalizar_uf).unique())
    if col_uf_obito and not df_o.empty: opcoes_uf.update(df_o[col_uf_obito].dropna().apply(normalizar_uf).unique())
    
    lista_ufs = sorted([x for x in opcoes_uf if x is not None])
    
    col_filtro, _ = st.columns([1, 3])
    with col_filtro:
        uf_selecionada = st.selectbox("🌍 Filtrar por Região/Estado:", ["BRASIL (Todos)"] + lista_ufs)

    # Aplica Filtro
    titulo_grafico = "Brasil (Consolidado)"
    if uf_selecionada != "BRASIL (Todos)":
        titulo_grafico = f"Estado: {uf_selecionada}"
        if col_uf_prod:
            df_p['_UF_NORM'] = df_p[col_uf_prod].apply(normalizar_uf)
            df_p = df_p[df_p['_UF_NORM'] == uf_selecionada]
        if col_uf_obito and not df_o.empty:
            df_o['_UF_NORM'] = df_o[col_uf_obito].apply(normalizar_uf)
            df_o = df_o[df_o['_UF_NORM'] == uf_selecionada]

    # --- 3. PROCESSAMENTO DE PRODUTOS (POR ANO) ---
    cols_ano = ['ANO', 'ANO_BASE', 'EXERCICIO']
    cols_data = ['DATA CRIACAO', 'DATA_CRIACAO', 'DT_CRIACAO', 'CRIADO EM', 'CREATED_AT', 'DATA']
    
    col_ano = next((c for c in cols_ano if c in df_p.columns), None)
    df_p['ANO_FINAL'] = None

    if col_ano:
        df_p['ANO_FINAL'] = df_p[col_ano]
    else:
        col_data = next((c for c in cols_data if c in df_p.columns), None)
        if col_data:
            df_p['DATA_TEMP'] = pd.to_datetime(df_p[col_data], errors='coerce', dayfirst=True)
            df_p = df_p.dropna(subset=['DATA_TEMP'])
            df_p['ANO_FINAL'] = df_p['DATA_TEMP'].dt.year

    # Limpeza e Filtro Temporal (2018-2025)
    df_p['ANO_FINAL'] = pd.to_numeric(df_p['ANO_FINAL'], errors='coerce')
    df_p = df_p.dropna(subset=['ANO_FINAL'])
    df_p['ANO_FINAL'] = df_p['ANO_FINAL'].astype(int)
    df_p = df_p[(df_p['ANO_FINAL'] >= 2018) & (df_p['ANO_FINAL'] <= 2025)]

    df_prod_ano = df_p.groupby('ANO_FINAL').size().reset_index(name='Qtd_Produtos')
    df_prod_ano.rename(columns={'ANO_FINAL': 'Ano'}, inplace=True)

    # --- 4. PROCESSAMENTO DE ÓBITOS (USANDO TOTAL_ANUAL) ---
    if df_o.empty:
        df_obitos_ano = pd.DataFrame(columns=['Ano', 'Obitos'])
    else:
        # Identifica Ano
        col_ano_obito = next((c for c in ['ano_uid', 'ano', 'ano_nome'] if c in df_o.columns), None)
        
        if col_ano_obito:
            df_o['Ano'] = df_o[col_ano_obito].astype(str).str.replace(r'\D', '', regex=True)
            df_o['Ano'] = pd.to_numeric(df_o['Ano'], errors='coerce').fillna(0).astype(int)

            # === AQUI ESTÁ A LÓGICA PEDIDA: USA COLUNA TOTAL_ANUAL ===
            if 'total_anual' in df_o.columns:
                # Converte para numérico para garantir a soma correta
                df_o['Obitos'] = pd.to_numeric(df_o['total_anual'], errors='coerce').fillna(0)
            else:
                # Fallback: Se não existir total_anual, soma os meses
                meses = ['janeiro', 'fevereiro', 'marco', 'abril', 'maio', 'junho', 
                         'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro']
                cols_meses = [c for c in meses if c in df_o.columns]
                df_o['Obitos'] = df_o[cols_meses].sum(axis=1) if cols_meses else 0
            
            # Agrupa por Ano (Soma o total de todos os registros daquele ano)
            df_obitos_ano = df_o.groupby('Ano')['Obitos'].sum().reset_index()
        else:
            df_obitos_ano = pd.DataFrame(columns=['Ano', 'Obitos'])

    # --- 5. UNIFICAÇÃO E GRÁFICO ---
    df_final = pd.merge(df_prod_ano, df_obitos_ano, on='Ano', how='outer').fillna(0).sort_values('Ano')
    df_final = df_final[(df_final['Ano'] >= 2018) & (df_final['Ano'] <= 2025)]

    if df_final.empty:
        st.info("Sem dados coincidentes para o período.")
        return

    # Correlação
    correlacao = df_final['Qtd_Produtos'].corr(df_final['Obitos'])
    
    # KPIs
    c1, c2, c3 = st.columns(3)
    delta_prod = df_final['Qtd_Produtos'].iloc[-1] - df_final['Qtd_Produtos'].iloc[0] if len(df_final)>1 else 0
    delta_obito = df_final['Obitos'].iloc[-1] - df_final['Obitos'].iloc[0] if len(df_final)>1 else 0

    c1.metric("Evolução Entregas", f"{delta_prod:+.0f}")
    c2.metric("Evolução Óbitos", f"{delta_obito:+.0f}", delta_color="inverse")
    
    texto_corr = "Sem correlação"
    if correlacao < -0.5: texto_corr = "✅ Correlação Negativa (Ideal)"
    elif correlacao > 0.5: texto_corr = "⚠️ Correlação Positiva (Alerta)"
    
    c3.metric("Correlação", f"{correlacao:.2f}", texto_corr)

    # Gráfico Dual Axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(go.Scatter(
        x=df_final['Ano'], y=df_final['Qtd_Produtos'], name="Produtos",
        mode='lines+markers+text', text=df_final['Qtd_Produtos'], textposition='top center',
        line=dict(color='#3366CC', width=4)), secondary_y=False)

    fig.add_trace(go.Scatter(
        x=df_final['Ano'], y=df_final['Obitos'], name="Óbitos",
        mode='lines+markers+text',
        text=df_final['Obitos'].apply(lambda x: f"{x:,.0f}"), # Formatação numérica simples
        textposition='bottom center',
        line=dict(color='#DC3912', width=4, dash='dot'), marker=dict(symbol='diamond')), secondary_y=True)

    fig.update_layout(
        title=f"<b>Evolução 2018-2025:</b> Entregas vs. Óbitos - {titulo_grafico}",
        hovermode="x unified", legend=dict(orientation="h", y=1.1), height=500
    )
    fig.update_xaxes(title_text="Ano", type='category')
    fig.update_yaxes(title_text="Produtos", showgrid=False, secondary_y=False)
    fig.update_yaxes(title_text="Óbitos", showgrid=True, secondary_y=True)

    st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)

    with st.expander("📋 Ver Tabela de Dados"):
        st.dataframe(df_final.set_index('Ano'), use_container_width=True)