import streamlit as st
import pandas as pd
import plotly.express as px

# Configuración de página
st.set_page_config(page_title="Phaser.js Game Analytics", layout="wide")
st.title("🎮 Game Analytics: Performance vs Sentiment")
st.markdown("Análisis de la relación entre dificultad y el estado emocional del jugador.")

# 1. Conexión a Snowflake
# Streamlit maneja el caché de los queries con el parámetro ttl (Time To Live)
@st.cache_data(ttl=600)
def load_data(query):
    conn = st.connection("snowflake", type="snowflake")
    return conn.query(query)

DB = "GOOD_GAME_DB" 
SCHEMA = "GOLD"

try:
    df_players = load_data(f"SELECT * FROM {DB}.{SCHEMA}.GOLD_PLAYER_METRICS")
    df_levels = load_data(f"SELECT * FROM {DB}.{SCHEMA}.GOLD_LEVEL_ANALYSIS ORDER BY LEVEL")
    df_sentiment = load_data(f"SELECT * FROM {DB}.{SCHEMA}.GOLD_SENTIMENT_PERFORMANCE WHERE UNIFIED_SENTIMENT_SCORE IS NOT NULL")

except Exception as e:
    st.error(f"Error conectando a Snowflake: {e}")
    st.stop()

# 2. Métricas Generales (Top KPIs)
st.subheader("General Overview")
col1, col2, col3, col4 = st.columns(4)
total_players = df_players['PLAYER_ID'].nunique()
avg_sessions = df_players['TOTAL_SESSIONS'].mean()

col1.metric("Total Players", total_players)
col2.metric("Avg Sessions per Player (Stickiness)", round(avg_sessions, 2))
col3.metric("Global Avg Completion Rate", f"{df_levels['COMPLETION_RATE'].mean() * 100:.1f}%")
col4.metric("Avg Sentiment Score", round(df_sentiment['UNIFIED_SENTIMENT_SCORE'].mean(), 2))

st.divider()

# 3. Gráficos Principales
col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.subheader("Deaths vs Sentiment Analysis")
    st.markdown("¿Cómo impacta la dificultad en el humor del jugador?")
    
    # Mapeo de colores manual para que coincidan con el nombre del mood
    color_map = {
        "green": "#2ca02c", 
        "blue": "#1f77b4", 
        "yellow": "#bcbd22", 
        "orange": "#ff7f0e", 
        "red": "#d62728"
    }

    fig_scatter = px.scatter(
        df_sentiment, 
        x='DEATHS', 
        y='UNIFIED_SENTIMENT_SCORE', 
        color='COLOR_MOOD',
        color_discrete_map=color_map, # Aplicamos nuestros colores
        facet_col='COLOR_MOOD',       # <--- ¡ESTO CREA MINI GRÁFICOS!
        facet_col_wrap=3,             # Organiza en 3 columnas
        opacity=0.4,                  # Puntos más suaves
        hover_data=['LEVEL', 'WORD'],
        trendline="ols",
        labels={
            'DEATHS': 'Deaths', 
            'UNIFIED_SENTIMENT_SCORE': 'Score (1-5)',
            'COLOR_MOOD': 'Mood'
        },
        template="plotly_dark"
    )

    # Mejorar el diseño estético
    fig_scatter.update_layout(
        showlegend=False,
        margin=dict(l=20, r=20, t=40, b=20),
        height=500
    )
    
    st.plotly_chart(fig_scatter, use_container_width=True)

with col_chart2:
    st.subheader("Level Retention Funnel")
    st.markdown("Caída de jugadores por nivel (Churn)")
    
    # Calcular embudo de retención
    funnel_data = df_sentiment.groupby('LEVEL')['PLAYER_ID'].nunique().reset_index()
    funnel_data.rename(columns={'PLAYER_ID': 'UNIQUE_PLAYERS'}, inplace=True)
    
    fig_funnel = px.funnel(
        funnel_data, 
        x='UNIQUE_PLAYERS', 
        y='LEVEL', 
        orientation='h',
        labels={'UNIQUE_PLAYERS': 'Players', 'LEVEL': 'Level'}
    )
    st.plotly_chart(fig_funnel, use_container_width=True)

st.divider()

# 4. Análisis de Sentimiento (Palabras)
st.subheader("Word Analysis by Mood")
st.markdown("Filtra el feedback de texto dejado por los jugadores según su color de ánimo reportado.")

# Filtro en la UI
available_colors = df_sentiment['COLOR_MOOD'].dropna().unique().tolist()
selected_color = st.selectbox("Filter by Color Mood:", ["All"] + available_colors)

# Aplicar filtro
if selected_color == "All":
    df_words = df_sentiment[['PLAYER_ID', 'LEVEL', 'COLOR_MOOD', 'WORD', 'DEATHS']].dropna(subset=['WORD'])
else:
    df_words = df_sentiment[df_sentiment['COLOR_MOOD'] == selected_color][['PLAYER_ID', 'LEVEL', 'COLOR_MOOD', 'WORD', 'DEATHS']].dropna(subset=['WORD'])

# Limpiar filas vacías
df_words = df_words[df_words['WORD'].str.strip() != ""]

# Mostrar tabla interactiva (ideal para dashboards analíticos en lugar de wordclouds estáticos)
st.dataframe(df_words, use_container_width=True, hide_index=True)