import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import snowflake.connector

def run_pyspark_dw_job():

    # 1. Validación de entorno
    required_vars = ["SF_ACCOUNT", "SF_USER", "SF_PASSWORD", "SF_DATABASE", "SF_SCHEMA", "SF_WAREHOUSE"]
    env = {var: os.getenv(var) for var in required_vars}
    
    if any(not val for val in env.values()):
        print(f"Error: Faltan variables de entorno.")
        return

    # 2. Opciones de Spark-Snowflake
    sf_options = {
        "sfURL": f"https://{env['SF_ACCOUNT']}.snowflakecomputing.com",
        "sfUser": env['SF_USER'],
        "sfPassword": env['SF_PASSWORD'],
        "sfDatabase": env['SF_DATABASE'],
        "sfSchema": env['SF_SCHEMA'],
        "sfWarehouse": env['SF_WAREHOUSE'],
        "sfRole": "ACCOUNTADMIN"
    }

    spark = SparkSession.builder.appName("DW_Incremental_Load").getOrCreate()

    # 3. CONFIGURACIÓN REAL (Basada en el diagnóstico previo)
    tablas_config = {
        "players": {
            "date_col": "CREATED_AT", 
            "default": "'1900-01-01'::TIMESTAMP"
        },
        "game_sessions": {
            "date_col": "CREATED_AT", 
            "default": "'1900-01-01'::TIMESTAMP"
        },
        "sentiment_responses": {
            "date_col": "CREATED_AT", 
            "default": "'1900-01-01'::TIMESTAMP"
        },
        "checkpoint_events": {
            "date_col": "ID", 
            "default": "0"
        }
    }

    # 4. Conexión nativa para marcas de agua
    try:
        conn = snowflake.connector.connect(
            user=env['SF_USER'], password=env['SF_PASSWORD'], account=env['SF_ACCOUNT'],
            warehouse=env['SF_WAREHOUSE'], database=env['SF_DATABASE'], schema=env['SF_SCHEMA']
        )
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error de conexión: {e}")
        return

    # 5. Procesamiento
    for tabla, config in tablas_config.items():
        print(f"\nProcesando: {tabla.upper()}")
        col_sf = config['date_col']
        
        try:
            # A. Obtener marca de agua
            cursor.execute(f"SELECT COALESCE(MAX({col_sf}), {config['default']}) FROM {tabla}")
            ultima_marca = cursor.fetchone()[0]
            print(f"--> Punto de control actual ({col_sf}): {ultima_marca}")

            # B. Leer Parquet
            path = f"data_output/{tabla}_temp"
            if not os.path.exists(path):
                print(f" No hay archivos para {tabla}")
                continue

            df_parquet = spark.read.parquet(path)

            # C. Match de columna en Spark (Ignorar case)
            col_spark = [c for c in df_parquet.columns if c.upper() == col_sf][0]

            # D. Filtrar y Cargar
            df_nuevos = df_parquet.filter(col(col_spark) > ultima_marca)
            conteo = df_nuevos.count()

            if conteo > 0:
                print(f"Cargando {conteo} registros...")
                df_nuevos.write \
                    .format("snowflake") \
                    .options(**sf_options) \
                    .option("dbtable", tabla) \
                    .mode("append") \
                    .save()
                print(f"Tabla {tabla} actualizada.")
            else:
                print(f" Sin datos nuevos para {tabla}.")

        except Exception as e:
            print(f"Error en {tabla}: {e}")

    cursor.close()
    conn.close()
    spark.stop()
    print("\nProceso completado")

if __name__ == "__main__":
    run_pyspark_dw_job()
