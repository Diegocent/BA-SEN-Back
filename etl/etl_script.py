import pandas as pd
import psycopg2
from data_cleaner import DataCleaner
from datetime import datetime
import os
from pathlib import Path

# Mapeo de meses en español (mayúsculas) para evitar dependencias de locale
MONTHS_ES = {
    1: 'ENERO', 2: 'FEBRERO', 3: 'MARZO', 4: 'ABRIL', 5: 'MAYO', 6: 'JUNIO',
    7: 'JULIO', 8: 'AGOSTO', 9: 'SEPTIEMBRE', 10: 'OCTUBRE', 11: 'NOVIEMBRE', 12: 'DICIEMBRE'
}

# Intentar cargar un archivo .env si python-dotenv está instalado
try:
    from dotenv import load_dotenv
    script_dir = Path(__file__).resolve().parent
    env_candidates = [script_dir / '.env', script_dir.parent / '.env']
    for p in env_candidates:
        if p.exists():
            load_dotenv(p)
            print(f"⚙️ Cargando variables de entorno desde {p}")
            break
except Exception:
    pass

# --- Configuración desde variables de entorno (con valores por defecto) ---
DB_DW_NAME = os.environ.get("DB_DW_NAME", "data_warehouse")
DB_DW_USER = os.environ.get("DB_DW_USER", "postgres")
DB_DW_PASS = os.environ.get("DB_DW_PASS", "postgres")
DB_DW_HOST = os.environ.get("DB_DW_HOST", "localhost")
DB_DW_PORT = os.environ.get("DB_DW_PORT", "5432")

# --- Conexión a la base de datos ---
def get_db_connection(db_name, db_user, db_password, db_host, db_port):
    """Establece la conexión a la base de datos."""
    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        print("✅ Conexión a Data Warehouse exitosa.")
        return conn
    except psycopg2.Error as e:
        print(f"❌ No se pudo conectar a Data Warehouse: {e}")
        return None

# --- Extracción ---
def extract_data_from_excel(file_path="registros_historicos.xlsx", sheet_name="Hoja1"):
    """Carga los datos desde un archivo Excel local."""
    path = Path(file_path)
    if not path.exists():
        path = Path(__file__).resolve().parent.parent / file_path
    
    if not path.exists():
        print(f"❌ Archivo no encontrado en {file_path} ni en el directorio padre.")
        return None

    print(f"🔍 Extrayendo datos desde: {path}")
    try:
        df = pd.read_excel(path, sheet_name=sheet_name)
        print(f"✅ Extracción completa. {len(df)} registros cargados.")
        return df
    except Exception as e:
        print(f"❌ Error al leer el archivo Excel: {e}")
        return None

# --- Transformación/Limpieza ---
def clean_data(df):
    """Limpia y estandariza los datos usando el pipeline robusto."""
    print("\n🧹 INICIANDO LIMPIEZA Y TRANSFORMACIÓN ROBUSTA...")
    cleaner = DataCleaner()
    
    df_cleaned = cleaner.run_complete_correction_pipeline(df)
    
    cleaner.verificacion_final(df_cleaned)

    # Convertir a lista de diccionarios para la carga
    cleaned_records = df_cleaned.to_dict('records')
    print(f"✅ Limpieza y transformación completa. {len(cleaned_records)} registros listos para cargar.")
    return cleaned_records


# --- Carga (CORREGIDA PARA ESQUEMA DIMENSIONAL) ---
def load_data_to_dw(conn_dw, cleaned_records):
    """Implementa la lógica de Carga Dimensional (Dimensiones -> Hechos)."""
    cursor = conn_dw.cursor()
    
    # Convertir a DataFrame para simplificar la carga dimensional
    df_cleaned = pd.DataFrame(cleaned_records)
    
    # 1. TRUNCATE DE LA TABLA DE HECHOS
    try:
        cursor.execute("TRUNCATE TABLE hechos_asistencia_humanitaria RESTART IDENTITY;")
        print("✅ Tabla hechos_asistencia_humanitaria truncada exitosamente.")
        conn_dw.commit()
    except Exception as e:
        print(f"❌ Error al truncar la tabla de hechos: {e}")
        conn_dw.rollback()
        raise e
    
    # 2. CARGA DE DIMENSIONES (INSERT OR IGNORE/DO NOTHING)
    print("⏳ Iniciando carga de Dimensiones (Fecha, Evento, Ubicación)...")

    # --- DIM_FECHA ---
    # Convertir las fechas a tipo date para el mapeo con PostgreSQL
    df_cleaned['FECHA_DATE'] = df_cleaned['FECHA'].dt.date 
    df_dates = df_cleaned[['FECHA_DATE', 'AÑO', 'MES', 'FECHA']].drop_duplicates(subset=['FECHA_DATE'])
    
    for index, row in df_dates.iterrows():
        try:
            date_obj = row['FECHA']
            # Usar el mapeo en español para el nombre del mes (evita problemas de locale)
            try:
                mes_num = int(date_obj.month)
            except Exception:
                # Si no es un datetime válido, caer de vuelta al formato por defecto
                mes_num = None

            nombre_mes = MONTHS_ES.get(mes_num, date_obj.strftime("%B").upper() if mes_num else 'SIN ESPECIFICAR')
            
            insert_query = """
                INSERT INTO dim_fecha (fecha, anio, mes, nombre_mes, dia_del_mes)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (fecha) DO NOTHING;
            """
            cursor.execute(insert_query, (row['FECHA_DATE'], row['AÑO'], row['MES'], nombre_mes, date_obj.day))
        except Exception as e:
            conn_dw.rollback()
            
    # --- DIM_EVENTO ---
    df_events = df_cleaned[['EVENTO']].drop_duplicates()
    for evento in df_events['EVENTO']:
        try:
            insert_query = "INSERT INTO dim_evento (evento) VALUES (%s) ON CONFLICT (evento) DO NOTHING;"
            cursor.execute(insert_query, (evento,))
        except Exception as e:
            conn_dw.rollback()

    # --- DIM_UBICACION ---
    df_locations = df_cleaned[['DEPARTAMENTO', 'DISTRITO', 'LOCALIDAD', 'ORDEN_DEPARTAMENTO']].drop_duplicates()
    for index, row in df_locations.iterrows():
        try:
            insert_query = """
                INSERT INTO dim_ubicacion (departamento, distrito, localidad, orden)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (departamento, distrito, localidad) DO NOTHING;
            """
            cursor.execute(insert_query, (row['DEPARTAMENTO'], row['DISTRITO'], row['LOCALIDAD'], row['ORDEN_DEPARTAMENTO']))
        except Exception as e:
            conn_dw.rollback()

    conn_dw.commit() # Commit después de cargar todas las dimensiones
    print("✅ Carga de dimensiones completada.")


    # 3. CARGA DE HECHOS (LOOKUP + INSERT)
    print("⏳ Iniciando carga de Tabla de Hechos (hechos_asistencia_humanitaria)...")
    registros_cargados = 0
    
    # Pre-cargar Lookups en memoria
    # Usamos read_sql que requiere 'import pandas as pd'
    dim_fecha_map = pd.read_sql("SELECT id_fecha, fecha FROM dim_fecha", conn_dw).set_index('fecha')['id_fecha'].to_dict()
    dim_evento_map = pd.read_sql("SELECT id_evento, evento FROM dim_evento", conn_dw).set_index('evento')['id_evento'].to_dict()
    
    # Lookup de ubicación (combinando las claves naturales)
    dim_ubicacion_map_df = pd.read_sql("SELECT id_ubicacion, departamento, distrito, localidad FROM dim_ubicacion", conn_dw)
    dim_ubicacion_map_df['key'] = dim_ubicacion_map_df['departamento'] + '|' + dim_ubicacion_map_df['distrito'] + '|' + dim_ubicacion_map_df['localidad']
    dim_ubicacion_lookup = dim_ubicacion_map_df.set_index('key')['id_ubicacion'].to_dict()
    
    insert_query_fact = """
        INSERT INTO hechos_asistencia_humanitaria (
            id_fecha, id_ubicacion, id_evento, 
            kit_sentencia, kit_evento, chapa_fibrocemento_cantidad, 
            chapa_zinc_cantidad, colchones_cantidad, frazadas_cantidad, 
            terciadas_cantidad, puntales_cantidad, carpas_plasticas_cantidad
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    for rec in df_cleaned.to_dict('records'):
        try:
            # Lookup de ID's
            id_fecha = dim_fecha_map.get(rec['FECHA'].to_pydatetime().date())
            id_evento = dim_evento_map.get(rec['EVENTO'])
            ubicacion_key = rec['DEPARTAMENTO'] + '|' + rec['DISTRITO'] + '|' + rec['LOCALIDAD']
            id_ubicacion = dim_ubicacion_lookup.get(ubicacion_key)

            # Mapeo de KIT_A/KIT_B a kit_sentencia/kit_evento
            # Asumiendo: KIT_A -> kit_sentencia, KIT_B -> kit_evento
            kit_sentencia = rec.get('KIT_A')
            kit_evento = rec.get('KIT_B')
            
            if id_fecha and id_evento and id_ubicacion:
                 cursor.execute(
                    insert_query_fact,
                    (
                        id_fecha, id_ubicacion, id_evento, 
                        kit_sentencia, kit_evento, rec.get('CHAPA_FIBROCEMENTO'), 
                        rec.get('CHAPA_ZINC'), rec.get('COLCHONES'), rec.get('FRAZADAS'), 
                        rec.get('TERCIADAS'), rec.get('PUNTALES'), rec.get('CARPAS_PLASTICAS')
                    )
                )
                 registros_cargados += 1
            
        except Exception as e:
            # Manejar errores de un registro específico
            print(f"❌ Error al cargar registro en Hechos (Depto: {rec.get('DEPARTAMENTO')}, Evento: {rec.get('EVENTO')}): {e}")
            conn_dw.rollback()
    
    conn_dw.commit()
    print(f"✅ Carga de datos completa. {registros_cargados} registros cargados en hechos_asistencia_humanitaria.")


def main():
    # 1. Extraer datos
    df = extract_data_from_excel()
    if df is None:
        return
    
    # 2. Limpiar datos (Ahora devuelve la lista de diccionarios)
    cleaned_records = clean_data(df)
    if not cleaned_records:
        print("❌ No hay registros limpios para cargar")
        return
    
    # 3. Conectar al Data Warehouse local
    conn_dw = get_db_connection(DB_DW_NAME, DB_DW_USER, DB_DW_PASS, DB_DW_HOST, DB_DW_PORT)
    if not conn_dw:
        return

    try:
        # 4. Cargar datos al Data Warehouse con lógica Dimensional
        load_data_to_dw(conn_dw, cleaned_records)
    except Exception as e:
        print(f"\n🛑 ETL fallido. Error en la fase de carga: {e}")
    finally:
        if conn_dw:
            conn_dw.close()
            print("❌ Conexión a Data Warehouse cerrada.")

if __name__ == '__main__':
    main()