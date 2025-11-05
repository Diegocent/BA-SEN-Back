import pandas as pd
import psycopg2
from data_cleaner import DataCleaner
from datetime import datetime
import os
from pathlib import Path

# Mapeo simple de número de mes -> nombre del mes en español.
# Se usa para llenar la columna `nombre_mes` en `dim_fecha` sin depender
# de la configuración regional del sistema.
MONTHS_ES = {
    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
    7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
}

# Intentar cargar un archivo .env si python-dotenv está disponible.
# Se buscan `.env` en el directorio del script y en su padre.
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
    # Si python-dotenv no está instalado o ocurre un error, seguimos con las env ya disponibles
    pass

# Leer credenciales y configuración desde variables de entorno.
# Si no están definidas, se usan valores por defecto para desarrollo.
DB_DW_NAME = os.environ.get("DB_DW_NAME", "data_warehouse")
DB_DW_USER = os.environ.get("DB_DW_USER", "postgres")
DB_DW_PASS = os.environ.get("DB_DW_PASS", "postgres")
DB_DW_HOST = os.environ.get("DB_DW_HOST", "localhost")
DB_DW_PORT = os.environ.get("DB_DW_PORT", "5432")

# --- Conexión a la base de datos ---
def get_db_connection(db_name, db_user, db_password, db_host, db_port):
    """Crear una conexión psycopg2 al DW. Devuelve None si falla."""
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

# Extracción: carga datos desde un Excel local
def extract_data_from_excel(file_path="registros_historicos.xlsx", sheet_name="Hoja 1"):
    """Busca el archivo en el directorio del script (o en su padre) y lee la hoja indicada."""
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

# Transformación y limpieza: aplica el pipeline de `DataCleaner`.
def clean_data(df):
    """Estandariza, infiere eventos y prepara los registros para la carga."""
    print("\n🧹 INICIANDO LIMPIEZA Y TRANSFORMACIÓN ROBUSTA...")
    cleaner = DataCleaner()
    
    df_cleaned = cleaner.run_complete_correction_pipeline(df)
    
    cleaner.verificacion_final(df_cleaned)

    # Convertir a lista de diccionarios para la carga
    cleaned_records = df_cleaned.to_dict('records')
    print(f"✅ Limpieza y transformación completa. {len(cleaned_records)} registros listos para cargar.")
    return cleaned_records


# Carga: inserta dimensiones y luego la tabla de hechos siguiendo un esquema dimensional.
def load_data_to_dw(conn_dw, cleaned_records):
    """Inserta/actualiza dimensiones (fecha, evento, ubicación) y carga la tabla de hechos."""
    cursor = conn_dw.cursor()
    
    # Convertir la lista de registros a DataFrame para operaciones de agrupado y lookups
    df_cleaned = pd.DataFrame(cleaned_records)
    # Asegurar que las columnas de ubicación y evento sean strings limpias y consistentes
    for c in ['DEPARTAMENTO', 'DISTRITO', 'LOCALIDAD', 'EVENTO']:
        if c in df_cleaned.columns:
            df_cleaned[c] = df_cleaned[c].fillna('SIN ESPECIFICAR').astype(str).str.strip().str.upper()
    
    # 1. TRUNCATE DE LA TABLA DE HECHOS
    try:
        cursor.execute("TRUNCATE TABLE hechos_asistencia_humanitaria RESTART IDENTITY;")
        print("✅ Tabla hechos_asistencia_humanitaria truncada exitosamente.")
        conn_dw.commit()
    except Exception as e:
        print(f"❌ Error al truncar la tabla de hechos: {e}")
        conn_dw.rollback()
        raise e
    
    # 2. CARGA DE DIMENSIONES: fecha, evento y ubicación.
    print("⏳ Iniciando carga de dimensiones (dim_fecha, dim_evento, dim_ubicacion)...")

    # --- DIM_FECHA ---
    # Preparar las filas únicas de fecha. `FECHA_DATE` es la representación tipo date
    # que se usará como clave natural en la dimensión.
    df_cleaned['FECHA_DATE'] = df_cleaned['FECHA'].dt.date 
    df_dates = df_cleaned[['FECHA_DATE', 'AÑO', 'MES', 'FECHA']].drop_duplicates(subset=['FECHA_DATE'])
    
    for index, row in df_dates.iterrows():
        try:
            date_obj = row['FECHA']
            # Obtener número de mes desde el objeto datetime; si falla dejamos None.
            try:
                mes_num = int(date_obj.month)
            except Exception:
                mes_num = None

            # Usar el mapeo en español; si no se encuentra, cae en un fallback seguro.
            nombre_mes = MONTHS_ES.get(mes_num, date_obj.strftime("%B").upper() if mes_num else 'SIN ESPECIFICAR')
            
            insert_query = """
                INSERT INTO dim_fecha (fecha, anio, mes, nombre_mes, dia_del_mes)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (fecha) DO NOTHING;
            """
            cursor.execute(insert_query, (row['FECHA_DATE'], row['AÑO'], row['MES'], nombre_mes, date_obj.day))
        except Exception as e:
            # Log del error para diagnóstico (no detener todo el proceso)
            conn_dw.rollback()
            print(f"❌ Error insertando dim_fecha para FECHA={row.get('FECHA_DATE')}: {e}")
            # Continuar con las demás filas
            pass
    # Commit parcial después de intentar insertar todas las fechas
    conn_dw.commit()

    # Diagnóstico: cuántas fechas únicas intentamos insertar vs cuántas quedaron en la tabla
    try:
        df_dim_fecha_check = pd.read_sql("SELECT fecha FROM dim_fecha", conn_dw)
        df_dim_fecha_check['fecha'] = pd.to_datetime(df_dim_fecha_check['fecha']).dt.date
        fechas_insertadas_db = set(df_dim_fecha_check['fecha'].tolist())
        fechas_intentadas = set(df_dates['FECHA_DATE'].dropna().tolist())
        fechas_faltantes = sorted(list(fechas_intentadas - fechas_insertadas_db))
        print(f"  Fechas únicas intentadas: {len(fechas_intentadas)}; Fechas en dim_fecha: {len(fechas_insertadas_db)}")
        if fechas_faltantes:
            print(f"  Ejemplos de fechas intentadas que NO están en dim_fecha (hasta 20): {fechas_faltantes[:20]}")
    except Exception:
        pass
            
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


    # 3. CARGA DE HECHOS: resolver FK mediante lookups en memoria e insertar filas.
    print("⏳ Iniciando carga de la tabla de hechos (hechos_asistencia_humanitaria)...")
    registros_cargados = 0
    missing_fecha = 0
    missing_evento = 0
    missing_ubicacion = 0
    missing_ubicacion_keys = {}
    
    # Pre-cargar lookups (dim_fecha, dim_evento, dim_ubicacion) en memoria para acelerar inserts.
    # read_sql devuelve DataFrame, por eso se usa pandas aquí.
    df_dim_fecha = pd.read_sql("SELECT id_fecha, fecha FROM dim_fecha", conn_dw)
    # Asegurar que las claves sean objetos date (sin componente horaria) para matching
    if 'fecha' in df_dim_fecha.columns:
        df_dim_fecha['fecha'] = pd.to_datetime(df_dim_fecha['fecha']).dt.date
    dim_fecha_map = df_dim_fecha.set_index('fecha')['id_fecha'].to_dict()
    df_dim_evento = pd.read_sql("SELECT id_evento, evento FROM dim_evento", conn_dw)
    if 'evento' in df_dim_evento.columns:
        df_dim_evento['evento'] = df_dim_evento['evento'].fillna('SIN ESPECIFICAR').astype(str).str.strip().str.upper()
    dim_evento_map = df_dim_evento.set_index('evento')['id_evento'].to_dict()
    
    # Lookup de ubicación: construir una clave natural concatenada departamento|distrito|localidad
    dim_ubicacion_map_df = pd.read_sql("SELECT id_ubicacion, departamento, distrito, localidad FROM dim_ubicacion", conn_dw)
    # Normalizar campos leídos desde la dimensión para asegurar coincidencia
    for c in ['departamento', 'distrito', 'localidad']:
        if c in dim_ubicacion_map_df.columns:
            dim_ubicacion_map_df[c] = dim_ubicacion_map_df[c].fillna('SIN ESPECIFICAR').astype(str).str.strip().str.upper()
    dim_ubicacion_map_df['key'] = dim_ubicacion_map_df['departamento'] + '|' + dim_ubicacion_map_df['distrito'] + '|' + dim_ubicacion_map_df['localidad']
    dim_ubicacion_lookup = dim_ubicacion_map_df.set_index('key')['id_ubicacion'].to_dict()
    # Mapa secundario por (departamento, distrito) para fallback si localidad no coincide
    try:
        dim_ubicacion_by_dept_dist = dim_ubicacion_map_df.set_index(['departamento', 'distrito'])['id_ubicacion'].to_dict()
    except Exception:
        dim_ubicacion_by_dept_dist = {}
    
    insert_query_fact = """
        INSERT INTO hechos_asistencia_humanitaria (
            id_fecha, id_ubicacion, id_evento, 
            kit_sentencia, kit_evento, chapa_fibrocemento_cantidad, 
            chapa_zinc_cantidad, colchones_cantidad, frazadas_cantidad, 
            terciadas_cantidad, puntales_cantidad, carpas_plasticas_cantidad
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    # Iterar registros limpios y encolar inserts en la tabla de hechos
    for rec in df_cleaned.to_dict('records'):
        try:
            # Resolver IDs usando los mapas precargados
            # Resolver la fecha del registro de manera robusta (acepta Timestamp, datetime, date o str)
            rec_fecha_raw = rec.get('FECHA')
            rec_fecha = None
            try:
                if hasattr(rec_fecha_raw, 'to_pydatetime'):
                    rec_fecha = rec_fecha_raw.to_pydatetime().date()
                elif isinstance(rec_fecha_raw, datetime):
                    rec_fecha = rec_fecha_raw.date()
                else:
                    rec_fecha = pd.to_datetime(rec_fecha_raw).date()
            except Exception:
                rec_fecha = None

            id_fecha = dim_fecha_map.get(rec_fecha)
            id_evento = dim_evento_map.get(rec['EVENTO'])
            ubicacion_key = rec['DEPARTAMENTO'] + '|' + rec['DISTRITO'] + '|' + rec['LOCALIDAD']
            id_ubicacion = dim_ubicacion_lookup.get(ubicacion_key)
            # Fallback: si no hay match por localidad, intentar por (departamento, distrito)
            if not id_ubicacion:
                dept_key = (str(rec.get('DEPARTAMENTO', '')).strip().upper(), str(rec.get('DISTRITO', '')).strip().upper())
                id_ubicacion = dim_ubicacion_by_dept_dist.get(dept_key)

            # Sumar las cantidades de kits de los campos KIT_A y KIT_B (ya limpiados y en el DF)
            # Asumimos que si no están presentes, son 0. KIT_A y KIT_B ya contienen enteros limpios.
            kit_a_qty = rec.get('KIT_A', 0)
            kit_b_qty = rec.get('KIT_B', 0)
            total_kits = kit_a_qty + kit_b_qty
            
            # Aplicar la regla: kit_sentencia SOLO si el EVENTO es C.I.D.H.
            if rec.get('EVENTO') == 'C.I.D.H.':
                kit_sentencia = total_kits
                kit_evento = 0
            else:
                kit_sentencia = 0
                kit_evento = total_kits

            # Insertar solo si se pudieron resolver las 3 FK necesarias
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
            else:
                # Contabilizar por tipo de fallo para diagnóstico
                if not id_fecha:
                    missing_fecha += 1
                if not id_evento:
                    missing_evento += 1
                if not id_ubicacion:
                    missing_ubicacion += 1
                    # guardar ejemplos de claves no resueltas
                    missing_ubicacion_keys[ubicacion_key] = missing_ubicacion_keys.get(ubicacion_key, 0) + 1

        except Exception as e:
            # Manejar errores por registro: se hace rollback para ese registro y seguimos
            print(f"❌ Error al cargar registro en Hechos (Depto: {rec.get('DEPARTAMENTO')}, Evento: {rec.get('EVENTO')}): {e}")
            conn_dw.rollback()
    
    conn_dw.commit()
    print(f"✅ Carga de datos completa. {registros_cargados} registros cargados en hechos_asistencia_humanitaria.")
    # Diagnóstico de registros no insertados por fallos en FK
    total_processed = len(df_cleaned)
    total_failed = total_processed - registros_cargados
    print(f"  Registros procesados: {total_processed}")
    print(f"  Registros insertados: {registros_cargados}")
    print(f"  Registros no insertados: {total_failed}")
    print(f"    - Fallos por fecha (id_fecha faltante): {missing_fecha}")
    print(f"    - Fallos por evento (id_evento faltante): {missing_evento}")
    print(f"    - Fallos por ubicacion (id_ubicacion faltante): {missing_ubicacion}")
    if missing_ubicacion_keys:
        print("  Ejemplos de claves de ubicacion no resueltas (clave -> ocurrencias):")
        # Mostrar hasta 20 ejemplos ordenados por ocurrencias
        for k, v in sorted(missing_ubicacion_keys.items(), key=lambda x: -x[1])[:20]:
            print(f"    {k} -> {v}")


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