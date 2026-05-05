from pyspark.sql import SparkSession
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

def run_job():
    print("Iniciando proceso masivo de tablas...")
    
    # 1. Configurar Spark
    spark = SparkSession.builder \
        .appName("AzureToServerSnowflake") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("Error")
    
    tablas = [
        "checkpoint_events",
        "game_sessions",
        "players", 
        "sentiment_responses"
    ]
    
    jdbc_url = "jdbc:postgresql://psql-goodgame.postgres.database.azure.com:5432/postgres?sslmode=require"
    
    # Conectamos a Snowflake una sola vez al principio
    print("Conectando a Snowflake...")
    conn = snowflake.connector.connect(
        user=os.getenv('SF_USER'),
        password=os.getenv('SF_PASSWORD'),
        account=os.getenv('SF_ACCOUNT'),
        database=os.getenv('SF_DATABASE'),
        schema=os.getenv('SF_SCHEMA'),
        warehouse=os.getenv('SF_WAREHOUSE')
    )
    
    for tabla in tablas:
        print(f"\n--- Procesando tabla: {tabla} ---")
        
        # A) Leer de Postgres (usamos la variable 'tabla')
        df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", tabla) \
        .option("user", os.getenv('PG_USER')) \
        .option("password", os.getenv('PG_PASSWORD')) \
        .option("driver", "org.postgresql.Driver") \
        .load()
            
        conteo = df.count()
        print(f"--> Filas encontradas en Postgres: {conteo}")

        if conteo == 0:
            print(f"¡La tabla {tabla} está vacía! Pasando a la siguiente...")
            continue # Salta a la siguiente tabla

        # B) Guardar en Parquet (crea una carpenta para cada tabla)
        temp_path = f"data_output/{tabla}_temp"
        df.write.mode("overwrite").parquet(temp_path)
        print("--> Archivos Parquet generados.")
        
        # C) Subir a Snowflake (en carpetas separadas dentro del Stage)
        print("--> Subiendo archivos al Stage...")
        archivos_subidos = 0
        for file in os.listdir(temp_path):
            if file.endswith(".parquet"):
                full_path = os.path.abspath(os.path.join(temp_path, file))
                # Nota: @stage_gg_azure/{tabla} separa los archivos organizadamente
                conn.cursor().execute(f"PUT file://{full_path} @stage_gg_azure/{tabla} OVERWRITE = TRUE")
                archivos_subidos += 1
                
        print(f"--> ¡Listo! Se subieron {archivos_subidos} archivos de {tabla}.")

    conn.close():
    print("\n¡Migración de todas las tablas completada!")

if __name__ == "__main__":
    run_job()
