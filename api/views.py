import json
from django.http import JsonResponse
from django.db import connections
from django.db.models import Sum

# Función de utilidad para ejecutar consultas en el DW y devolver una lista de diccionarios
def _execute_query_and_fetch(query, filters=None, pagination=None):
    """
    Ejecuta una consulta SQL en el Data Warehouse con filtros y paginación.
    Retorna una lista de diccionarios, junto con el total de registros.
    """
    try:
        with connections['data_warehouse'].cursor() as cursor:
            # Construir las cláusulas WHERE a partir de los filtros
            where_clauses = []
            general_search_term = None
            
            # Separar el filtro de búsqueda general del resto
            if filters and 'inputBusqueda' in filters:
                general_search_term = filters.pop('inputBusqueda')

            if filters:
                for key, value in filters.items():
                    if value is not None:
                        # Para valores de texto, usar LIKE para una búsqueda flexible
                        where_clauses.append(f"{key} = '{value}'")
            
            # Manejar la búsqueda general
            if general_search_term:
                search_like_clause = []
                # Columnas relevantes para la búsqueda general
                search_columns = ['du.departamento', 'du.distrito', 'du.localidad', 'de.evento']
                for col in search_columns:
                    search_like_clause.append(f"{col} ILIKE '%%{general_search_term}%%'")
                where_clauses.append("(" + " OR ".join(search_like_clause) + ")")

            where_string = ""
            if where_clauses:
                where_string = "WHERE " + " AND ".join(where_clauses)
            
            # Contar el total de registros antes de aplicar la paginación
            # Se usa el query original para contar
            count_query = f"SELECT COUNT(*) FROM ({query.replace(';', '')} {where_string}) as subquery;"
            cursor.execute(count_query)
            total_records = cursor.fetchone()[0]

            # Aplicar paginación
            pagination_string = ""
            if pagination:
                limit = pagination.get('limit')
                offset = pagination.get('offset')
                if limit is not None and offset is not None:
                    pagination_string = f"LIMIT {limit} OFFSET {offset}"

            final_query = f"{query.replace(';', '')} {where_string} {pagination_string};"
            cursor.execute(final_query)
            
            columns = [col[0] for col in cursor.description]
            data = [dict(zip(columns, row)) for row in cursor.fetchall()]

            return data, total_records
    except Exception as e:
        print(f"Error al ejecutar la consulta en el DW: {e}")
        return [], 0

# -------------------- ENDPOINTS DE LA API --------------------

def api_asistencias_anual(request):
    """
    Endpoint API que devuelve las cantidades de ayuda por año.
    Soporta paginación.
    """
    try:
        page = int(request.GET.get('page', 1))
        per_page = int(request.GET.get('per_page', 10))
    except (ValueError, TypeError):
        return JsonResponse({'error': 'Parámetros de paginación no válidos'}, status=400)
    
    offset = (page - 1) * per_page
    pagination = {'limit': per_page, 'offset': offset}

    query = """
    SELECT
        df.anio AS anio,
        SUM(h.kit_sentencia) AS kit_sentencia,
        SUM(h.kit_evento) AS kit_evento,
        SUM(h.chapa_fibrocemento_cantidad + h.chapa_zinc_cantidad) AS chapas
    FROM hechos_asistencia_humanitaria h
    JOIN dim_fecha df ON h.id_fecha = df.id_fecha
    GROUP BY df.anio
    ORDER BY df.anio
    """
    data, total_records = _execute_query_and_fetch(query, pagination=pagination)
    
    if not data:
        return JsonResponse({'error': 'No data available'}, status=404)
        
    return JsonResponse({
        'data': data,
        'total': total_records,
        'page': page,
        'per_page': per_page,
        'total_pages': (total_records + per_page - 1) // per_page
    }, safe=False)

def api_asistencias_mensual(request):
    """
    Endpoint API que devuelve las cantidades de ayuda por mes.
    Soporta filtros por año y paginación.
    """
    try:
        page = int(request.GET.get('page', 1))
        per_page = int(request.GET.get('per_page', 10))
    except (ValueError, TypeError):
        return JsonResponse({'error': 'Parámetros de paginación no válidos'}, status=400)
    
    offset = (page - 1) * per_page
    pagination = {'limit': per_page, 'offset': offset}
    
    anio = request.GET.get('anio')
    input_busqueda = request.GET.get('inputBusqueda')
    filters = {'df.anio': anio, 'inputBusqueda': input_busqueda}

    query = """
    SELECT
        df.anio AS anio,
        df.mes AS mes,
        df.nombre_mes AS nombre_mes,
        SUM(h.kit_sentencia) AS kit_sentencia,
        SUM(h.kit_evento) AS kit_evento,
        SUM(h.chapa_fibrocemento_cantidad + h.chapa_zinc_cantidad) AS chapas
    FROM hechos_asistencia_humanitaria h
    JOIN dim_fecha df ON h.id_fecha = df.id_fecha
    GROUP BY df.anio, df.mes, df.nombre_mes
    ORDER BY df.anio, df.mes
    """
    data, total_records = _execute_query_and_fetch(query, filters=filters, pagination=pagination)
    
    if not data:
        return JsonResponse({'error': 'No data available'}, status=404)
        
    return JsonResponse({
        'data': data,
        'total': total_records,
        'page': page,
        'per_page': per_page,
        'total_pages': (total_records + per_page - 1) // per_page
    }, safe=False)

def api_asistencias_por_ubicacion(request):
    """
    Endpoint API que devuelve las asistencias por departamento y distrito.
    Soporta filtros y paginación.
    """
    try:
        page = int(request.GET.get('page', 1))
        per_page = int(request.GET.get('per_page', 10))
    except (ValueError, TypeError):
        return JsonResponse({'error': 'Parámetros de paginación no válidos'}, status=400)
    
    offset = (page - 1) * per_page
    pagination = {'limit': per_page, 'offset': offset}

    departamento = request.GET.get('departamento')
    distrito = request.GET.get('distrito')
    input_busqueda = request.GET.get('inputBusqueda')
    filters = {'du.departamento': departamento, 'du.distrito': distrito, 'inputBusqueda': input_busqueda}

    query = """
    SELECT
        du.departamento,
        du.distrito,
        SUM(h.kit_sentencia) AS kit_sentencia,
        SUM(h.kit_evento) AS kit_evento,
        SUM(h.chapa_fibrocemento_cantidad + h.chapa_zinc_cantidad) AS chapas
    FROM hechos_asistencia_humanitaria h
    JOIN dim_ubicacion du ON h.id_ubicacion = du.id_ubicacion
    GROUP BY du.departamento, du.distrito
    ORDER BY kit_sentencia DESC
    """
    data, total_records = _execute_query_and_fetch(query, filters=filters, pagination=pagination)
    
    if not data:
        return JsonResponse({'error': 'No data available'}, status=404)
        
    return JsonResponse({
        'data': data,
        'total': total_records,
        'page': page,
        'per_page': per_page,
        'total_pages': (total_records + per_page - 1) // per_page
    }, safe=False)

def api_asistencias_por_evento(request):
    """
    Endpoint API para el análisis por tipo de evento, incluyendo conteo de ocurrencias.
    Soporta paginación.
    """
    try:
        page = int(request.GET.get('page', 1))
        per_page = int(request.GET.get('per_page', 10))
    except (ValueError, TypeError):
        return JsonResponse({'error': 'Parámetros de paginación no válidos'}, status=400)
    
    offset = (page - 1) * per_page
    pagination = {'limit': per_page, 'offset': offset}
    
    input_busqueda = request.GET.get('inputBusqueda')
    filters = {'inputBusqueda': input_busqueda}

    query = """
    SELECT
        de.evento AS tipoEvento,
        COUNT(h.id_asistencia_hum) AS numeroOcurrencias,
        SUM(h.kit_sentencia) AS kit_sentencia,
        SUM(h.kit_evento) AS kit_evento,
        SUM(h.chapa_fibrocemento_cantidad + h.chapa_zinc_cantidad) AS chapas
    FROM hechos_asistencia_humanitaria h
    JOIN dim_evento de ON h.id_evento = de.id_evento
    GROUP BY de.evento
    ORDER BY numeroOcurrencias DESC
    """
    data, total_records = _execute_query_and_fetch(query, filters=filters, pagination=pagination)

    if not data:
        return JsonResponse({'error': 'No data available'}, status=404)
        
    return JsonResponse({
        'data': data,
        'total': total_records,
        'page': page,
        'per_page': per_page,
        'total_pages': (total_records + per_page - 1) // per_page
    }, safe=False)

def api_eventos_por_departamento(request):
    """
    Endpoint API que devuelve las cantidades de ayuda para eventos específicos en cada departamento.
    Soporta filtros y paginación.
    """
    try:
        page = int(request.GET.get('page', 1))
        per_page = int(request.GET.get('per_page', 10))
    except (ValueError, TypeError):
        return JsonResponse({'error': 'Parámetros de paginación no válidos'}, status=400)
    
    offset = (page - 1) * per_page
    pagination = {'limit': per_page, 'offset': offset}

    departamento = request.GET.get('departamento')
    input_busqueda = request.GET.get('inputBusqueda')
    filters = {'du.departamento': departamento, 'inputBusqueda': input_busqueda}

    query = """
    SELECT
        du.departamento,
        de.evento,
        SUM(h.kit_sentencia) as kit_sentencia,
        SUM(h.kit_evento) as kit_evento,
        SUM(h.chapa_fibrocemento_cantidad + h.chapa_zinc_cantidad) AS chapas
    FROM hechos_asistencia_humanitaria h
    JOIN dim_ubicacion du ON h.id_ubicacion = du.id_ubicacion
    JOIN dim_evento de ON h.id_evento = de.id_evento
    GROUP BY du.departamento, de.evento
    ORDER BY du.departamento, de.evento
    """
    data, total_records = _execute_query_and_fetch(query, filters=filters, pagination=pagination)
    
    if not data:
        return JsonResponse({'error': 'No data available'}, status=404)
        
    return JsonResponse({
        'data': data,
        'total': total_records,
        'page': page,
        'per_page': per_page,
        'total_pages': (total_records + per_page - 1) // per_page
    }, safe=False)

def api_registros_detallados(request):
    """
    Endpoint API que devuelve el detalle de los registros, incluyendo la ubicación,
    el evento y las cantidades de ayuda. Soporta filtros y paginación.
    """
    try:
        page = int(request.GET.get('page', 1))
        per_page = int(request.GET.get('per_page', 10))
    except (ValueError, TypeError):
        return JsonResponse({'error': 'Parámetros de paginación no válidos'}, status=400)
    
    offset = (page - 1) * per_page
    pagination = {'limit': per_page, 'offset': offset}
    
    departamento = request.GET.get('departamento')
    distrito = request.GET.get('distrito')
    evento = request.GET.get('evento')
    localidad = request.GET.get('localidad')
    input_busqueda = request.GET.get('inputBusqueda')
    filters = {
        'du.departamento': departamento,
        'du.distrito': distrito,
        'de.evento': evento,
        'du.localidad': localidad,
        'inputBusqueda': input_busqueda
    }

    query = """
    SELECT
        df.fecha,
        du.departamento,
        du.distrito,
        du.localidad,
        de.evento,
        h.kit_sentencia as kit_sentencia,
        h.kit_evento as kit_evento,
        (h.chapa_fibrocemento_cantidad + h.chapa_zinc_cantidad) AS chapas
    FROM hechos_asistencia_humanitaria h
    JOIN dim_fecha df ON h.id_fecha = df.id_fecha
    JOIN dim_ubicacion du ON h.id_ubicacion = du.id_ubicacion
    JOIN dim_evento de ON h.id_evento = de.id_evento
    ORDER BY df.fecha DESC
    """
    data, total_records = _execute_query_and_fetch(query, filters=filters, pagination=pagination)
    
    if not data:
        return JsonResponse({'error': 'No data available'}, status=404)
        
    return JsonResponse({
        'data': data,
        'total': total_records,
        'page': page,
        'per_page': per_page,
        'total_pages': (total_records + per_page - 1) // per_page
    }, safe=False)