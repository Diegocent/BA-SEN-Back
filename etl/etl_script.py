import pandas as pd
import psycopg2
from data_cleaner import DataCleaner
from datetime import datetime
import os
from pathlib import Path
import sys
from datetime import datetime as _dt

# --- Guardar todo lo que se imprime en consola en un archivo ---
class Tee:
    """Escribe simultáneamente en varios 'file-like' (útil para 'tee' en consola).

    Se usa para mantener la salida en consola y al mismo tiempo volcarla a un archivo
    de seguimiento (logs) para pruebas y auditoría.
    """
    def __init__(self, *files):
        self.files = files

    def write(self, data):
        for f in self.files:
            try:
                f.write(data)
            except Exception:
                # No romper si un handler falla
                pass

    def flush(self):
        for f in self.files:
            try:
                f.flush()
            except Exception:
                pass


# Crear carpeta de logs en el mismo directorio del script
try:
    _logs_dir = Path(__file__).resolve().parent / 'logs'
    _logs_dir.mkdir(parents=True, exist_ok=True)
    _log_filename = f"etl_run_{_dt.now().strftime('%Y%m%d_%H%M%S')}.txt"
    _log_path = _logs_dir / _log_filename
    # Abrimos en modo append para no truncar en caso de re-ejecuciones rápidas
    _log_file = open(_log_path, 'a', encoding='utf-8')
    sys.stdout = Tee(sys.stdout, _log_file)
    sys.stderr = Tee(sys.stderr, _log_file)
    print(f"🔖 Log de ejecución iniciado en: {_log_path}")
except Exception as _e:
    # Si no podemos crear el log, continuamos pero avisamos por consola
    print(f"⚠️ No se pudo inicializar el archivo de log: {_e}")


# MAPEO DE MESES: Diccionario para nombres de meses en español
# Esto asegura consistencia en reportes independientemente de la configuración regional del servidor
MONTHS_ES = {
    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
    7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
}

# CONFIGURACIÓN DE VARIABLES DE ENTORNO: Carga segura de credenciales
# Esto sigue mejores prácticas de seguridad evitando hardcode de passwords
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
    # Fallback silencioso si dotenv no está disponible
    pass

# CONFIGURACIÓN DE CONEXIÓN A BASE DE DATOS
# Valores por defecto para desarrollo local - producción usa variables de entorno
DB_DW_NAME = os.environ.get("DB_DW_NAME", "data_warehouse")
DB_DW_USER = os.environ.get("DB_DW_USER", "postgres")
DB_DW_PASS = os.environ.get("DB_DW_PASS", "postgres")
DB_DW_HOST = os.environ.get("DB_DW_HOST", "localhost")
DB_DW_PORT = os.environ.get("DB_DW_PORT", "5432")

# --- FASE DE EXTRACCIÓN: Obtener datos desde fuentes externas ---
def get_db_connection(db_name, db_user, db_password, db_host, db_port):
    """Establece conexión con el Data Warehouse para la fase de CARGA.
    
    En CRISP-DM, esta función corresponde a la PREPARACIÓN DE DATOS,
    específicamente la conexión con el repositorio de datos de destino.
    """
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

def extract_data_from_excel(file_path="registros_historicos.xlsx", sheet_name="Hoja 1"):
    """Extrae datos desde archivo Excel - Fase EXTRACCIÓN del ETL.
    
    En CRISP-DM, esta es la fase de COMPRENSIÓN DE DATOS donde:
    - Identificamos la fuente de datos
    - Cargamos los datos crudos
    - Preparamos para el análisis inicial de calidad
    
    Busca archivos en múltiples ubicaciones para flexibilidad en desarrollo.
    """
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

# --- FASE DE TRANSFORMACIÓN: Limpieza y enriquecimiento de datos ---
def clean_data(df):
    """Aplica el pipeline completo de limpieza y transformación.
    
    En CRISP-DM, esta es la fase central de PREPARACIÓN DE DATOS que incluye:
    - Limpieza de valores inconsistentes
    - Estandarización de formatos
    - Enriquecimiento con información derivada
    - Validación de reglas de negocio
    
    Utiliza la clase DataCleaner que encapsula toda la lógica de transformación.
    """
    print("\n🧹 INICIANDO LIMPIEZA Y TRANSFORMACIÓN ROBUSTA...")
    cleaner = DataCleaner()
    
    # Ejecutar pipeline completo de corrección
    df_cleaned = cleaner.run_complete_correction_pipeline(df)
    
    # Verificación final de calidad (fase EVALUACIÓN en CRISP-DM)
    cleaner.verificacion_final(df_cleaned)

    # Convertir a formato list-of-dicts para facilitar la carga
    cleaned_records = df_cleaned.to_dict('records')
    print(f"✅ Limpieza y transformación completa. {len(cleaned_records)} registros listos para cargar.")
    return cleaned_records

# --- FASE DE CARGA: Inserción en el Data Warehouse ---
def load_data_to_dw(conn_dw, cleaned_records):
    """Carga datos al Data Warehouse usando modelo dimensional.
    
    En CRISP-DM, esta es la fase de MODELADO donde:
    - Implementamos el esquema dimensional (Hechos y Dimensiones)
    - Establecemos las relaciones entre tablas
    - Preparamos los datos para análisis OLAP
    
    Sigue el patrón clásico: cargar dimensiones primero, luego hechos.
    """
    cursor = conn_dw.cursor()
    
    # PREPARACIÓN: Convertir a DataFrame para operaciones de agrupamiento
    df_cleaned = pd.DataFrame(cleaned_records)
    
    # NORMALIZACIÓN DE TEXTO: Asegurar consistencia en campos clave
    for c in ['DEPARTAMENTO', 'DISTRITO', 'LOCALIDAD', 'EVENTO']:
        if c in df_cleaned.columns:
            df_cleaned[c] = df_cleaned[c].fillna('SIN ESPECIFICAR').astype(str).str.strip().str.upper()
    
    # 1. LIMPIEZA INICIAL: Truncar tabla de hechos para carga incremental completa
    try:
        cursor.execute("TRUNCATE TABLE hechos_asistencia_humanitaria RESTART IDENTITY;")
        print("✅ Tabla hechos_asistencia_humanitaria truncada exitosamente.")
        conn_dw.commit()
    except Exception as e:
        print(f"❌ Error al truncar la tabla de hechos: {e}")
        conn_dw.rollback()
        raise e
    
    # 2. CARGA DE DIMENSIONES: Construir las tablas de lookup del modelo dimensional
    print("⏳ Iniciando carga de dimensiones (dim_fecha, dim_evento, dim_ubicacion)...")

    # --- DIMENSIÓN FECHA: Jerarquía temporal para análisis por tiempo ---
    df_cleaned['FECHA_DATE'] = df_cleaned['FECHA'].dt.date 
    df_dates = df_cleaned[['FECHA_DATE', 'AÑO', 'MES', 'FECHA']].drop_duplicates(subset=['FECHA_DATE'])
    
    for index, row in df_dates.iterrows():
        try:
            date_obj = row['FECHA']
            # Extraer componentes de fecha de manera robusta
            try:
                mes_num = int(date_obj.month)
            except Exception:
                mes_num = None

            # Usar mapeo en español para nombres de mes consistentes
            nombre_mes = MONTHS_ES.get(mes_num, date_obj.strftime("%B").upper() if mes_num else 'SIN ESPECIFICAR')
            
            insert_query = """
                INSERT INTO dim_fecha (fecha, anio, mes, nombre_mes, dia_del_mes)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (fecha) DO NOTHING;
            """
            cursor.execute(insert_query, (row['FECHA_DATE'], row['AÑO'], row['MES'], nombre_mes, date_obj.day))
        except Exception as e:
            # Manejo tolerante a fallos: continuar con otras fechas si una falla
            conn_dw.rollback()
            print(f"❌ Error insertando dim_fecha para FECHA={row.get('FECHA_DATE')}: {e}")
            pass

    conn_dw.commit()

    # DIAGNÓSTICO DE CALIDAD: Verificar integridad de la carga de fechas
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
            
    # --- DIMENSIÓN EVENTO: Catálogo de tipos de eventos para análisis por causa ---
    df_events = df_cleaned[['EVENTO']].drop_duplicates()
    for evento in df_events['EVENTO']:
        try:
            insert_query = "INSERT INTO dim_evento (evento) VALUES (%s) ON CONFLICT (evento) DO NOTHING;"
            cursor.execute(insert_query, (evento,))
        except Exception as e:
            conn_dw.rollback()

    # --- DIMENSIÓN UBICACIÓN: Jerarquía geográfica para análisis espacial ---
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

    conn_dw.commit()
    print("✅ Carga de dimensiones completada.")

    # 3. CARGA DE TABLA DE HECHOS: Métricas y medidas con relaciones a dimensiones
    print("⏳ Iniciando carga de la tabla de hechos (hechos_asistencia_humanitaria)...")
    registros_cargados = 0
    missing_fecha = 0
    missing_evento = 0
    missing_ubicacion = 0
    missing_ubicacion_keys = {}
    
    # OPTIMIZACIÓN: Precargar dimensiones en memoria para búsquedas rápidas
    # Esto mejora performance evitando consultas individuales a la base de datos
    
    # Mapa de Fechas
    df_dim_fecha = pd.read_sql("SELECT id_fecha, fecha FROM dim_fecha", conn_dw)
    if 'fecha' in df_dim_fecha.columns:
        df_dim_fecha['fecha'] = pd.to_datetime(df_dim_fecha['fecha']).dt.date
    dim_fecha_map = df_dim_fecha.set_index('fecha')['id_fecha'].to_dict()
    
    # Mapa de Eventos
    df_dim_evento = pd.read_sql("SELECT id_evento, evento FROM dim_evento", conn_dw)
    if 'evento' in df_dim_evento.columns:
        df_dim_evento['evento'] = df_dim_evento['evento'].fillna('SIN ESPECIFICAR').astype(str).str.strip().str.upper()
    dim_evento_map = df_dim_evento.set_index('evento')['id_evento'].to_dict()
    
    # Mapa de Ubicaciones - estructura compleja por jerarquía geográfica
    dim_ubicacion_map_df = pd.read_sql("SELECT id_ubicacion, departamento, distrito, localidad FROM dim_ubicacion", conn_dw)
    # Normalizar campos para matching consistente
    for c in ['departamento', 'distrito', 'localidad']:
        if c in dim_ubicacion_map_df.columns:
            dim_ubicacion_map_df[c] = dim_ubicacion_map_df[c].fillna('SIN ESPECIFICAR').astype(str).str.strip().str.upper()
    
    # Crear clave compuesta para búsqueda exacta
    dim_ubicacion_map_df['key'] = dim_ubicacion_map_df['departamento'] + '|' + dim_ubicacion_map_df['distrito'] + '|' + dim_ubicacion_map_df['localidad']
    dim_ubicacion_lookup = dim_ubicacion_map_df.set_index('key')['id_ubicacion'].to_dict()
    
    # Fallback: mapa por departamento-distrito para casos donde localidad no coincide
    try:
        dim_ubicacion_by_dept_dist = dim_ubicacion_map_df.set_index(['departamento', 'distrito'])['id_ubicacion'].to_dict()
    except Exception:
        dim_ubicacion_by_dept_dist = {}
    
    # QUERY de inserción para tabla de hechos
    insert_query_fact = """
        INSERT INTO hechos_asistencia_humanitaria (
            id_fecha, id_ubicacion, id_evento, 
            kit_sentencia, kit_evento, chapa_fibrocemento_cantidad, 
            chapa_zinc_cantidad, colchones_cantidad, frazadas_cantidad, 
            terciadas_cantidad, puntales_cantidad, carpas_plasticas_cantidad
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    # PROCESAMIENTO POR REGISTRO: Resolver FK y aplicar reglas de negocio
    for rec in df_cleaned.to_dict('records'):
        try:
            # RESOLUCIÓN DE FECHA: Manejo robusto de múltiples formatos datetime
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

            # BUSCAR LLAVES FORÁNEAS en mapas precargados
            id_fecha = dim_fecha_map.get(rec_fecha)
            id_evento = dim_evento_map.get(rec['EVENTO'])
            ubicacion_key = rec['DEPARTAMENTO'] + '|' + rec['DISTRITO'] + '|' + rec['LOCALIDAD']
            id_ubicacion = dim_ubicacion_lookup.get(ubicacion_key)
            
            # FALLBACK GEOGRÁFICO: Si no encuentra por localidad, buscar por departamento-distrito
            if not id_ubicacion:
                dept_key = (str(rec.get('DEPARTAMENTO', '')).strip().upper(), str(rec.get('DISTRITO', '')).strip().upper())
                id_ubicacion = dim_ubicacion_by_dept_dist.get(dept_key)

            # REGLA DE NEGOCIO: Distribución de kits según tipo de evento
            # C.I.D.H. usa kit_sentencia, otros eventos usan kit_evento
            kit_a_qty = rec.get('KIT_A', 0)
            kit_b_qty = rec.get('KIT_B', 0)
            total_kits = kit_a_qty + kit_b_qty
            
            if rec.get('EVENTO') == 'C.I.D.H.':
                kit_sentencia = total_kits
                kit_evento = 0
            else:
                kit_sentencia = 0
                kit_evento = total_kits

            # INSERTAR SOLO SI TENEMOS TODAS LAS LLAVES FORÁNEAS
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
                # CONTABILIZAR ERRORES PARA DIAGNÓSTICO
                if not id_fecha:
                    missing_fecha += 1
                if not id_evento:
                    missing_evento += 1
                if not id_ubicacion:
                    missing_ubicacion += 1
                    missing_ubicacion_keys[ubicacion_key] = missing_ubicacion_keys.get(ubicacion_key, 0) + 1

        except Exception as e:
            # MANEJO DE ERRORES POR REGISTRO: Rollback del registro individual
            print(f"❌ Error al cargar registro en Hechos (Depto: {rec.get('DEPARTAMENTO')}, Evento: {rec.get('EVENTO')}): {e}")
            conn_dw.rollback()
    
    conn_dw.commit()
    
    # REPORTE FINAL DE CARGA: Transparencia en el proceso
    print(f"✅ Carga de datos completa. {registros_cargados} registros cargados en hechos_asistencia_humanitaria.")
    total_processed = len(df_cleaned)
    total_failed = total_processed - registros_cargados
    print(f"  Registros procesados: {total_processed}")
    print(f"  Registros insertados: {registros_cargados}")
    print(f"  Registros no insertados: {total_failed}")
    print(f"    - Fallos por fecha (id_fecha faltante): {missing_fecha}")
    print(f"    - Fallos por evento (id_evento faltante): {missing_evento}")
    print(f"    - Fallos por ubicacion (id_ubicacion faltante): {missing_ubicacion}")
    
    # DIAGNÓSTICO DETALLADO: Ejemplos de problemas para investigación
    if missing_ubicacion_keys:
        print("  Ejemplos de claves de ubicacion no resueltas (clave -> ocurrencias):")
        for k, v in sorted(missing_ubicacion_keys.items(), key=lambda x: -x[1])[:20]:
            print(f"    {k} -> {v}")

# --- FLUJO PRINCIPAL ETL ---
def main():
    """Orquesta el proceso ETL completo siguiendo metodología CRISP-DM.
    
    Fases del proceso:
    1. EXTRACCIÓN: Obtener datos desde fuentes externas
    2. TRANSFORMACIÓN: Limpiar, estandarizar y enriquecer datos
    3. CARGA: Insertar en Data Warehouse con modelo dimensional
    
    Este flujo corresponde a las fases de PREPARACIÓN DE DATOS en CRISP-DM.
    """
    # 1. EXTRACCIÓN: Obtener datos crudos desde Excel
    df = extract_data_from_excel()
    if df is None:
        return
    
    # 2. TRANSFORMACIÓN: Aplicar limpieza y reglas de negocio
    cleaned_records = clean_data(df)
    if not cleaned_records:
        print("❌ No hay registros limpios para cargar")
        return
    
    # 3. CONEXIÓN: Establecer conexión con Data Warehouse
    conn_dw = get_db_connection(DB_DW_NAME, DB_DW_USER, DB_DW_PASS, DB_DW_HOST, DB_DW_PORT)
    if not conn_dw:
        return

    try:
        # 4. CARGA: Ejecutar proceso de carga dimensional
        load_data_to_dw(conn_dw, cleaned_records)
        print("\n🎉 ETL COMPLETADO EXITOSAMENTE")
        print("   Los datos están listos para análisis en el Data Warehouse")
    except Exception as e:
        print(f"\n🛑 ETL fallido. Error en la fase de carga: {e}")
    finally:
        # LIMPIEZA: Cerrar conexión siempre
        if conn_dw:
            conn_dw.close()
            print("❌ Conexión a Data Warehouse cerrada.")

# PUNTO DE ENTRADA: Ejecutar ETL cuando el script se corre directamente
if __name__ == '__main__':
    main()