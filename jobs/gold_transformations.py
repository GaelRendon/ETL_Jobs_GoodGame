import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def run_gold_transformations():
    print("Iniciando transformación de Capa GOLD en Snowflake...")
    # 1. Conexión a Snowflake

    conn = snowflake.connector.connect(
        user=os.getenv('SF_USER'),
        password=os.getenv('SF_PASSWORD'),
        account=os.getenv('SF_ACCOUNT'),
        warehouse=os.getenv('SF_WAREHOUSE'),
        database=os.getenv('SF_DATABASE'),
        schema=os.getenv('SF_SCHEMA') # Donde están tus tablas Silver
    )

    cursor = conn.cursor()

    # 2. Definición de las consultas 
    queries = [
        # --- 1. PERFORMANCE + SENTIMIENTO ---
        """
        CREATE OR REPLACE TABLE GOLD.GOLD_SENTIMENT_PERFORMANCE AS
        WITH SentimentMapped AS (
            SELECT 
                s.session_id, s.player_id, s.color_mood, s.emoji_mood, s.word,
                CASE s.emoji_mood WHEN 1 THEN 1 WHEN 2 THEN 2 WHEN 3 THEN 3 WHEN 4 THEN 4 WHEN 5 THEN 5 ELSE 3 END AS emoji_score,
                CASE LOWER(s.color_mood) WHEN 'red' THEN 1 WHEN 'gray' THEN 2 WHEN 'yellow' THEN 3 WHEN 'blue' THEN 4 WHEN 'green' THEN 5 ELSE 3 END AS color_score
            FROM PUBLIC.SENTIMENT_RESPONSES s WHERE s.skipped = FALSE
        )

        SELECT 
            g.id AS session_id, g.player_id, g.level, g.deaths, g.time_seconds, g.score, g.completed,
            sm.color_mood, sm.emoji_mood, sm.word, sm.emoji_score, sm.color_score,
            (sm.emoji_score + sm.color_score) / 2.0 AS unified_sentiment_score
        FROM PUBLIC.GAME_SESSIONS g
        LEFT JOIN SentimentMapped sm ON g.id = sm.session_id;
        """,


        # --- 2. MÉTRICAS POR JUGADOR ---

        """

        CREATE OR REPLACE TABLE GOLD.GOLD_PLAYER_METRICS AS

        SELECT 
            p.id AS player_id, p.display_name, COUNT(DISTINCT g.id) AS total_sessions,
            MAX(g.level) AS max_level_reached, AVG(g.deaths) AS avg_deaths_per_session,
            SUM(g.time_seconds) AS total_playtime_seconds,
            MODE(sp.unified_sentiment_score) AS dominant_sentiment_score
        FROM PUBLIC.PLAYERS p
        LEFT JOIN PUBLIC.GAME_SESSIONS g ON p.id = g.player_id
        LEFT JOIN GOLD.GOLD_SENTIMENT_PERFORMANCE sp ON g.id = sp.session_id
        GROUP BY 1, 2;

        """,

        # --- 3. ANÁLISIS DE DIFICULTAD POR NIVEL ---

        """
        CREATE OR REPLACE TABLE GOLD.GOLD_LEVEL_ANALYSIS AS
        SELECT 
            level, COUNT(DISTINCT id) AS total_attempts,
            SUM(IFF(completed, 1, 0)) / NULLIF(COUNT(*), 0) AS completion_rate,
            AVG(deaths) AS avg_deaths, AVG(time_seconds) AS avg_time_seconds,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY max_x_reached) AS p75_dropoff_x
        FROM PUBLIC.GAME_SESSIONS GROUP BY 1;
        """,

        # --- 4. EMBUDO DE RETENCIÓN (LEVEL FUNNEL) ---

        """
        CREATE OR REPLACE TABLE GOLD.GOLD_LEVEL_FUNNEL AS
        WITH LevelCounts AS (

            SELECT level, COUNT(DISTINCT player_id) as unique_players FROM PUBLIC.GAME_SESSIONS GROUP BY level
        )

        SELECT level, unique_players,

               LAG(unique_players) OVER (ORDER BY level) as prev_level_players,
               ROUND(unique_players / NULLIF(LAG(unique_players) OVER (ORDER BY level), 0) * 100, 2) AS retention_rate_pct
        FROM LevelCounts;

        """,


        # --- 5. SEGMENTACIÓN DE JUGADORES ---

        """

        CREATE OR REPLACE TABLE GOLD.GOLD_PLAYER_SEGMENTS AS
        SELECT *,
            CASE 
                WHEN total_sessions >= 10 THEN '1. Hardcore'
                WHEN total_sessions >= 4 THEN '2. Regular'
                ELSE '3. Casual'
            END AS player_type
        FROM GOLD.GOLD_PLAYER_METRICS;
        """,


        # --- 6. CORRELACIÓN FRUSTRACIÓN/SENTIMIENTO ---

        """

        CREATE OR REPLACE TABLE GOLD.GOLD_DIFFICULTY_SENTIMENT_CORRELATION AS
        SELECT 
            level,
            CASE WHEN deaths = 0 THEN '0 Muertes' WHEN deaths <= 3 THEN '1-3 Muertes' ELSE '4+ Muertes' END AS death_tier,
            AVG(unified_sentiment_score) AS avg_sentiment,
            COUNT(*) AS session_count
        FROM GOLD.GOLD_SENTIMENT_PERFORMANCE GROUP BY 1, 2;
        """,

        # --- 7. ANÁLISIS DE PUNTOS DE CONTROL ---

        """

        CREATE OR REPLACE TABLE GOLD.GOLD_CHECKPOINT_ANALYSIS AS
        SELECT 
            c.checkpoint_index,
            g.level, 
            COUNT(DISTINCT c.session_id) AS times_reached
        FROM PUBLIC.CHECKPOINT_EVENTS c
        JOIN PUBLIC.GAME_SESSIONS g ON c.session_id = g.id
        GROUP BY 1, 2;
        """
    ]


    # 3. Ejecución secuencial

    try:

        for i, sql in enumerate(queries, 1):
            print(f"Ejecutando transformación {i}...")
            cursor.execute(sql)
        print("Capa GOLD actualizada")
    except Exception as e:
        print(f"Error en la transformación: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_gold_transformations() 