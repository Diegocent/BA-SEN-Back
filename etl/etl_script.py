import pandas as pd
import psycopg2
from data_cleaner import DataCleaner
from datetime import datetime
from collections import defaultdict

# --- Configuración de la base de datos ---
# NOTA: Estos valores han sido actualizados con tu información de Neon.Tech
DB_SOURCE_NAME = "neondb"
DB_SOURCE_USER = "neondb_owner"
DB_SOURCE_PASS = "npg_9Hy4VLazYWfr"
DB_SOURCE_HOST = "ep-hidden-cloud-a8nxbcwc-pooler.eastus2.azure.neon.tech"
DB_SOURCE_PORT = "5432"

DB_DW_NAME = "data_warehouse"
DB_DW_USER = "neondb_owner"
DB_DW_PASS = "npg_4jZsgShV0xAd"
DB_DW_HOST = "ep-gentle-meadow-acqg6ndn-pooler.sa-east-1.aws.neon.tech"
DB_DW_PORT = "5432"

# --- Conexión a las bases de datos ---
def get_db_connection(db_name, db_user, db_pass, db_host, db_port):
    """Establece y retorna una conexión a la base de datos."""
    try:
        # Los parámetros sslmode y channel_binding se pasan directamente a la función,
        # no a través del argumento 'options' para evitar el error 'unsupported options'.
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_pass,
            host=db_host,
            port=db_port,
            sslmode="require",
            channel_binding="require"
        )
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos {db_name}: {e}")
        return None

# --- Lógica de Extracción y Limpieza (similar a tu views.py) ---
def extract_and_clean_data(conn_source):
    """Extrae datos de la base de datos de origen y los limpia."""
    print("Iniciando la extracción y limpieza de datos...")
    cleaner = DataCleaner()
    cursor = conn_source.cursor()
    cursor.execute("SELECT * FROM asistencia_humanitaria;")
    
    # Obtener los nombres de las columnas
    column_names = [desc[0] for desc in cursor.description]
    
    raw_data = []
    for row in cursor.fetchall():
        record = dict(zip(column_names, row))
        raw_data.append(record)
    
    # Limpiar y filtrar registros
    cleaned_records = []
    for rec in raw_data:
        cleaned = cleaner.limpiar_registro_completo(rec)
        # Filtrar: solo registros con insumos y que no sean ELIMINAR_REGISTRO
        if cleaned['evento'] != 'ELIMINAR_REGISTRO' and cleaner.tiene_insumos(cleaned):
            cleaned_records.append(cleaned)
    print(f"Se extrajeron, limpiaron y filtraron {len(cleaned_records)} registros válidos.")
    return cleaned_records

# --- Lógica de Carga (Load) ---
def load_data_to_dw(conn_dw, cleaned_records):
    """Carga los datos limpios en el Data Warehouse."""
    print("Iniciando la carga de datos en el Data Warehouse...")
    cursor = conn_dw.cursor()

    # --- Carga las tablas de dimensión primero ---
    unique_fechas = {rec['fecha'] for rec in cleaned_records if rec['fecha']}
    unique_ubicaciones = {(rec['departamento'], rec['distrito'], rec['localidad']) for rec in cleaned_records}
    unique_eventos = {rec['evento'] for rec in cleaned_records}

    # Cargar y obtener IDs para las dimensiones
    print("Cargando la dimensión de fecha...")
    fecha_ids = {}
    for fecha_obj in sorted(list(unique_fechas)):
        fecha_str = fecha_obj.strftime('%Y-%m-%d')
        
        # Uso de consulta parametrizada para prevenir inyección SQL
        cursor.execute("SELECT id_fecha FROM dim_fecha WHERE fecha = %s;", (fecha_str,))
        result = cursor.fetchone()
        
        if not result:
            nombres_meses = {
                1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
                7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
            }
            nombre_mes_espanol = nombres_meses.get(fecha_obj.month)
            
            cursor.execute(
                "INSERT INTO dim_fecha (fecha, anio, mes, nombre_mes, dia_del_mes) VALUES (%s, %s, %s, %s, %s) RETURNING id_fecha;",
                (fecha_obj, fecha_obj.year, fecha_obj.month, nombre_mes_espanol, fecha_obj.day)
            )
            fecha_ids[fecha_obj] = cursor.fetchone()[0]
        else:
            fecha_ids[fecha_obj] = result[0]
    print("Dimensión de fecha cargada.")

    print("Cargando la dimensión de ubicación...")
    ubicacion_ids = {}
    for dep, dis, loc in unique_ubicaciones:
        cursor.execute(
            "SELECT id_ubicacion FROM dim_ubicacion WHERE departamento = %s AND distrito = %s AND localidad = %s;",
            (dep, dis, loc)
        )
        result = cursor.fetchone()
        if not result:
            cursor.execute(
                "INSERT INTO dim_ubicacion (departamento, distrito, localidad) VALUES (%s, %s, %s) RETURNING id_ubicacion;",
                (dep, dis, loc)
            )
            ubicacion_ids[(dep, dis, loc)] = cursor.fetchone()[0]
        else:
            ubicacion_ids[(dep, dis, loc)] = result[0]
    print("Dimensión de ubicación cargada.")

    print("Cargando la dimensión de evento...")
    evento_ids = {}
    for evento in unique_eventos:
        cursor.execute(f"SELECT id_evento FROM dim_evento WHERE evento = %s;", (evento,))
        result = cursor.fetchone()
        if not result:
            cursor.execute("INSERT INTO dim_evento (evento) VALUES (%s) RETURNING id_evento;", (evento,))
            evento_ids[evento] = cursor.fetchone()[0]
        else:
            evento_ids[evento] = result[0]
    print("Dimensión de evento cargada.")

    # --- Carga la tabla de hechos ---
    print("Cargando la tabla de hechos...")
    for rec in cleaned_records:
        if not rec['fecha']:
            continue
        try:
            # Revisa si ya existe un registro de hecho para evitar duplicados en cargas completas
            cursor.execute(
                "SELECT id_asistencia_hum FROM hechos_asistencia_humanitaria WHERE id_fecha = %s AND id_ubicacion = %s AND id_evento = %s;",
                (fecha_ids[rec['fecha']], ubicacion_ids[(rec['departamento'], rec['distrito'], rec['localidad'])], evento_ids[rec['evento']])
            )
            if cursor.fetchone() is None:
                cursor.execute(
                    """
                    INSERT INTO hechos_asistencia_humanitaria (
                        id_fecha, id_ubicacion, id_evento, kit_evento, kit_sentencia, chapa_fibrocemento_cantidad,
                        chapa_zinc_cantidad, colchones_cantidad, frazadas_cantidad, terciadas_cantidad, puntales_cantidad,
                        carpas_plasticas_cantidad
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """,
                    (
                        fecha_ids[rec['fecha']],
                        ubicacion_ids[(rec['departamento'], rec['distrito'], rec['localidad'])],
                        evento_ids[rec['evento']],
                        rec['kit_eventos'], rec['kit_sentencia'], rec['chapa_fibrocemento'], rec['chapa_zinc'],
                        rec['colchones'], rec['frazadas'], rec['terciadas'], rec['puntales'],
                        rec['carpas_plasticas']
                    )
                )
        except Exception as e:
            print(f"Error al cargar registro: {e}")
            conn_dw.rollback() # Deshacer si ocurre un error
    
    conn_dw.commit()
    print("Carga de datos completa. Transacción confirmada.")


def main():
    conn_source = get_db_connection(DB_SOURCE_NAME, DB_SOURCE_USER, DB_SOURCE_PASS, DB_SOURCE_HOST, DB_SOURCE_PORT)
    if not conn_source:
        return
    
    conn_dw = get_db_connection(DB_DW_NAME, DB_DW_USER, DB_DW_PASS, DB_DW_HOST, DB_DW_PORT)
    if not conn_dw:
        conn_source.close()
        return

    try:
        cleaned_records = extract_and_clean_data(conn_source)
        if cleaned_records:
            load_data_to_dw(conn_dw, cleaned_records)
    finally:
        conn_source.close()
        conn_dw.close()
        print("Conexiones a bases de datos cerradas.")

if __name__ == "__main__":
    main()
