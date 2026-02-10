import streamlit as st
import plotly.express as px
from utils import html_card, padronizar_grafico

def render_rede(df_users, tema):
    st.markdown(f"### 👥 Colaboradores")
    if df_users.empty: return

    mask = df_users['PERFIL'].astype(str).str.contains('MASTER|APROVADOR', case=False, na=False)
    df = df_users[~mask].copy()

    k1, k2 = st.columns(2)
    with k1: st.markdown(html_card("Total", len(df), "Usuários", tema), unsafe_allow_html=True)
    with k2: st.markdown(html_card("Órgãos", df['ORGAO'].nunique(), "Conectados", tema), unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        fig = px.pie(df['PERFIL'].value_counts().reset_index(), values='count', names='PERFIL', hole=0.5)
        st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)
    with c2:
        top = df['UF'].value_counts().head(10).reset_index()
        fig = px.bar(top, x='UF', y='count')
        st.plotly_chart(padronizar_grafico(fig, tema), use_container_width=True)