import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def run_native_snowflake_load():
    print("Iniciando carga incremental nativa en Snowflake...")

    # 1. Validación de entorno
    required_vars = ["SF_ACCOUNT", "SF_USER", "SF_PASSWORD", "SF_DATABASE", "SF_SCHEMA", "SF_WAREHOUSE"]
    env = {var: os.getenv(var) for var in required_vars}
    
    if any(not val for val in env.values()):
        print("Error: Faltan variables de entorno.")
        return

    # 2. Configuración de tablas (Marca de agua)
    tablas_config = {
        "players": {"date_col": "CREATED_AT", "default": "'1900-01-01'::TIMESTAMP"},
        "game_sessions": {"date_col": "CREATED_AT", "default": "'1900-01-01'::TIMESTAMP"},
        "sentiment_responses": {"date_col": "CREATED_AT", "default": "'1900-01-01'::TIMESTAMP"},
        "checkpoint_events": {"date_col": "ID", "default": "0"}
    }

    # 3. Conexión a Snowflake
    try:
        conn = snowflake.connector.connect(
            user=env['SF_USER'], password=env['SF_PASSWORD'], account=env['SF_ACCOUNT'],
            warehouse=env['SF_WAREHOUSE'], database=env['SF_DATABASE'], schema=env['SF_SCHEMA']
        )
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error de conexión a Snowflake: {e}")
        return


    # 4. Procesamiento ELT
    for tabla, config in tablas_config.items():
        print(f"\n--- Procesando carga para: {tabla.upper()} ---")
        col_sf = config['date_col']
        
        try:
            # A. Obtener marca de agua actual en la tabla final
            cursor.execute(f"SELECT COALESCE(MAX({col_sf}), {config['default']}) FROM {tabla}")
            ultima_marca = cursor.fetchone()[0]
            
            if isinstance(ultima_marca, str) or 'TIMESTAMP' in config['default']:
                ultima_marca_format = f"'{ultima_marca}'"
            else:
                ultima_marca_format = ultima_marca

            print(f"--> Punto de control actual ({col_sf}): {ultima_marca}")

            # B. Crear una tabla temporal idéntica a la tabla final y vaciarla
            cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE {tabla}_temp CLONE {tabla}")
            cursor.execute(f"TRUNCATE TABLE {tabla}_temp")
            
            # C. Cargar TODO el Parquet a la tabla temporal
            # MATCH_BY_COLUMN_NAME es la magia que asocia automáticamente las columnas del parquet con la tabla
            cursor.execute(f"""
                COPY INTO {tabla}_temp
                FROM @stage_gg_azure/{tabla}
                FILE_FORMAT = (TYPE = PARQUET)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            """)
            
            # D. Insertar en la tabla final SOLO lo que sea más reciente que la marca de agua
            cursor.execute(f"""
                INSERT INTO {tabla}
                SELECT * FROM {tabla}_temp
                WHERE {col_sf} > {ultima_marca_format}
            """)
            
            filas_insertadas = cursor.rowcount
            print(f"--> ¡Carga exitosa! Se insertaron {filas_insertadas} registros nuevos en {tabla}.")

        except Exception as e:
            print(f"--> Error al cargar la tabla {tabla}: {e}")

    cursor.close()
    conn.close()
    print("\n¡Proceso de Data Warehouse completado!")

if __name__ == "__main__":
    run_native_snowflake_load()
