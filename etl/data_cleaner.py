# -*- coding: utf-8 -*-
"""Pipeline de limpieza y estandarización para el ETL.

Contiene reglas y utilidades para normalizar departamentos, eventos y
cantidades antes de cargar los datos al Data Warehouse.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
import re
warnings.filterwarnings('ignore')


class DataCleaner:
    def __init__(self):
        # Nombres históricos de campos (compatibilidad con datasets antiguos)
        self.original_kit_fields = ['kit_a', 'kit_b']

        # Campos estándar que pueden aparecer o ser generados por el pipeline
        self.aid_fields = [
            'kit_eventos', 'kit_sentencia', 'chapa_fibrocemento', 'chapa_zinc',
            'colchones', 'frazadas', 'terciadas', 'puntales', 'carpas_plasticas'
        ]

    # Orden numérico de departamentos (clave para reportes/ordenamiento)
        self.departamento_orden = {
            'CONCEPCIÓN': 1, 'SAN PEDRO': 2, 'CORDILLERA': 3, 'GUAIRÁ': 4,
            'CAAGUAZÚ': 5, 'CAAZAPÁ': 6, 'ITAPÚA': 7, 'MISIONES': 8,
            'PARAGUARÍ': 9, 'ALTO PARANÁ': 10, 'CENTRAL': 11, 'ÑEEMBUCÚ': 12,
            'AMAMBAY': 13, 'CANINDEYÚ': 14, 'PDTE. HAYES': 15, 'BOQUERON': 16,
            'ALTO PARAGUAY': 17, 'CAPITAL': 18
        }

    # Diccionario de normalización de nombres de departamentos.
    # Mapea variantes y errores comunes a un nombre estándar.
        self.estandarizacion_dept = {
            # Limpieza de variantes
            'ÑEEMBUCU': 'ÑEEMBUCÚ', 'ÑEEMBUCÙ': 'ÑEEMBUCÚ', 'ÑEMBUCU': 'ÑEEMBUCÚ',
            'Ñeembucu': 'ÑEEMBUCÚ', 'ÑEEMBUCÚ': 'ÑEEMBUCÚ',

            'ALTO PARANA': 'ALTO PARANÁ', 'ALTO PARANÀ': 'ALTO PARANÁ',
            'ALTO PNÀ': 'ALTO PARANÁ', 'ALTO PNÁ': 'ALTO PARANÁ', 'ALTO PY': 'ALTO PARANÁ',
            'Alto Parana': 'ALTO PARANÁ', 'ALTO PARANÁ': 'ALTO PARANÁ',

            'BOQUERÒN': 'BOQUERON', 'BOQUERÓN': 'BOQUERON', 'Boqueron': 'BOQUERON',
            'BOQUERON': 'BOQUERON',

            'CAAGUAZU': 'CAAGUAZÚ', 'CAAGUAZÙ': 'CAAGUAZÚ', 'Caaguazu': 'CAAGUAZÚ',
            'Caaguazú': 'CAAGUAZÚ', 'CAAGUAZÚ': 'CAAGUAZÚ',
            'CAAG-CANIND': 'CAAGUAZÚ', 'CAAG/CANIN': 'CAAGUAZÚ', 'CAAG/CANIND.': 'CAAGUAZÚ',
            'CAAGUAZU- ALTO PARANA': 'CAAGUAZÚ', 'CAAGUAZU/MISIONES': 'CAAGUAZÚ',
            'CAAGUAZU - CANINDEYU': 'CAAGUAZÚ', 'CAAGUAZU Y CANINDEYU': 'CAAGUAZÚ',
            'CAAGUAZU, CANINDEYU Y SAN PEDRO': 'CAAGUAZÚ',
            'CAAGUAZU, SAN PEDRO Y CANINDEYU': 'CAAGUAZÚ',
            'CAAGUAZU-GUAIRA Y SAN PEDRO': 'CAAGUAZÚ', 'CAAGUAZU-GUAIRA': 'CAAGUAZÚ',

            'CAAZAPA': 'CAAZAPÁ', 'CAAZAPÀ': 'CAAZAPÁ', 'Caazapa': 'CAAZAPÁ',
            'CAAZAPÁ': 'CAAZAPÁ', 'CAAZAPA - GUAIRA': 'CAAZAPÁ',
            'Caazapa - Guaira': 'CAAZAPÁ',

            'CANINDEYU': 'CANINDEYÚ', 'CANINDEYÙ': 'CANINDEYÚ', 'Canindeyu': 'CANINDEYÚ',
            'CANINDEYÚ': 'CANINDEYÚ', 'CANINDEYU - CAAGUAZU': 'CANINDEYÚ',
            'CANINDEYU Y SAN PEDRO': 'CANINDEYÚ',

            'CENT/CORDILL': 'CENTRAL', 'CENTR-CORD': 'CENTRAL', 'CENTRAL': 'CENTRAL',
            'CENTRAL-CORDILLERA': 'CENTRAL', 'CENTRAL/CAP': 'CENTRAL', 'CENTRAL/CAPITAL': 'CENTRAL',
            'CENTRAL/COR': 'CENTRAL', 'CENTRAL/CORD': 'CENTRAL', 'CENTRAL/CORD.': 'CENTRAL',
            'CENTRAL/CORDILLER': 'CENTRAL', 'CENTRAL/CORDILLERA': 'CENTRAL',
            'CENTRAL/PARAG.': 'CENTRAL', 'central': 'CENTRAL',

            'CONCEPCION': 'CONCEPCIÓN', 'CONCEPCIÒN': 'CONCEPCIÓN', 'Concepcion': 'CONCEPCIÓN',
            'CONCEPCIÓN': 'CONCEPCIÓN',

            'COORDILLERA': 'CORDILLERA', 'CORD./CENTRAL': 'CORDILLERA',
            'CORD/S.PEDRO': 'CORDILLERA', 'CORDILLERA': 'CORDILLERA',
            'CORDILLERA ARROYOS Y EST.': 'CORDILLERA', 'CORDILLERA Y SAN PEDRO': 'CORDILLERA',
            'CORDILLERACAACUPÈ': 'CORDILLERA', 'Cordillera': 'CORDILLERA',
            'CORDILLERA ARROYOS': 'CORDILLERA',

            'GUAIRA': 'GUAIRÁ', 'GUAIRÀ': 'GUAIRÁ', 'GUIARA': 'GUAIRÁ',
            'Guaira': 'GUAIRÁ', 'GUAIRÁ': 'GUAIRÁ',
            'GUAIRA - CAAZAPA': 'GUAIRÁ', 'Guaira - Caazapa': 'GUAIRÁ',

            'ITAPUA': 'ITAPÚA', 'ITAPUA- CAAGUAZU': 'ITAPÚA', 'ITAPÙA': 'ITAPÚA',
            'Itapua': 'ITAPÚA', 'ITAPÚA': 'ITAPÚA',

            'MISIONES YABEBYRY': 'MISIONES', 'Misiones': 'MISIONES', 'MISIONES': 'MISIONES',

            'PARAGUARI': 'PARAGUARÍ', 'PARAGUARI PARAGUARI': 'PARAGUARÍ',
            'PARAGUARÌ': 'PARAGUARÍ', 'Paraguari': 'PARAGUARÍ', 'PARAGUARÍ': 'PARAGUARÍ',
            'PARAGUARI - GUAIRA': 'PARAGUARÍ', 'Paraguari -  Guaira': 'PARAGUARÍ',
            'PARAGUARI - GUAIRA': 'PARAGUARÍ',

            'PDTE HAYES': 'PDTE. HAYES', 'PDTE HAYES S.PIRI-4 DE MAYO': 'PDTE. HAYES',
            'PDTE HYES': 'PDTE. HAYES', 'PDTE. HAYES': 'PDTE. HAYES', 'PTE HAYES': 'PDTE. HAYES',
            'PTE. HAYES': 'PDTE. HAYES', 'Pdte Hayes': 'PDTE. HAYES', 'Pdte. Hayes': 'PDTE. HAYES',
            'PDTE.HAYES': 'PDTE. HAYES',

            'S.PEDRO/CAN.': 'SAN PEDRO', 'SAN PEDRO': 'SAN PEDRO',
            'SAN PEDRO-CAAGUAZU': 'SAN PEDRO', 'SAN PEDRO/ AMAMBAY': 'SAN PEDRO',
            'SAN PEDRO/ CANINDEYU': 'SAN PEDRO', 'San Pedro': 'SAN PEDRO',
            'SAN PEDRO - CANINDEYU': 'SAN PEDRO', 'San Pedro - Canindeyu': 'SAN PEDRO',

            # CASOS ESPECIALES - TODOS A CENTRAL
            'VARIOS DEP.': 'CENTRAL', 'VARIOS DPTOS.': 'CENTRAL', 'VARIOS DPTS.': 'CENTRAL',
            'varios': 'CENTRAL', 'REGION ORIENTAL/ OCCIDENTAL': 'CENTRAL',
            'VARIOS': 'CENTRAL', 'ASOC MUSICO': 'CENTRAL', 'INDI': 'CENTRAL',
            'SIN_DEPARTAMENTO': 'CENTRAL', 'SIN ESPECIFICAR': 'CENTRAL',

            # DISTRITOS MAPEADOS A SUS DEPARTAMENTOS
            'CNEL OVIEDO': 'CAAGUAZÚ', 'ITA': 'CENTRAL', 'ITAUGUA': 'CENTRAL',
            'VILLARICA': 'GUAIRÁ', 'ASUNCION': 'CAPITAL', 'ASUNCIÓN': 'CAPITAL',
            'CAACUPÈ': 'CORDILLERA', 'CAACUPÉ': 'CORDILLERA',

            # DEPARTAMENTOS BASE
            'ALTO PARAGUAY': 'ALTO PARAGUAY', 'AMAMBAY': 'AMAMBAY', 'CAPITAL': 'CAPITAL'
        }

    # Diccionario de normalización de eventos.
    # Algunas entradas indican que el registro debe eliminarse (p. ej. preposicionamiento).
    self.estandarizacion_eventos = {
            # COVID
            'ALB.COVID': 'COVID', 'ALBER.COVID': 'COVID', 'ALBERG.COVID': 'COVID',
            'COVI 19 OLL.': 'COVID', 'COVID 19': 'COVID', 'COVI': 'COVID',
            'VAC.ARATIRI': 'COVID', 'VACUNATORIO SND': 'COVID',
            'APOY.INST.COVID 19': 'COVID', 'APOYO INSTITUCIONAL COVID': 'COVID',
            'ÑANGARECO': 'COVID', 'ÑANGAREKO': 'COVID',

            # INCENDIO
            'INC.FORESTAL': 'INCENDIO', 'INCCENDIO': 'INCENDIO', 'INCEND': 'INCENDIO',
            'INCEND. DOMIC.': 'INCENDIO', 'INCENDIO DOMICILIARIO': 'INCENDIO',
            'DERRUMBE': 'INCENDIO', 'INCENDIO FORESTAL': 'INCENDIO',

            # TORMENTA SEVERA
            'EVENTO CLIMATICO': 'TORMENTA SEVERA', 'TORMENTA SEVERA CENTRAL': 'TORMENTA SEVERA',
            'EVENTO CLIMATICO TEMPORAL': 'TORMENTA SEVERA', 'MUNICIPALIDAD': 'TORMENTA SEVERA',
            'TEMPORAL': 'TORMENTA SEVERA', 'TEMPORAL CENTRAL': 'TORMENTA SEVERA',
            'TEMPORAL - MUNICIPALIDAD': 'TORMENTA SEVERA', 'TEMPORAL-GOBERNACION': 'TORMENTA SEVERA',
            'TEMPORAL - GOBERNACION': 'TORMENTA SEVERA', 'TEMPORAL-GOBERNACIÓN': 'TORMENTA SEVERA',
            'TEMPORAL - GOBERNACIÓN': 'TORMENTA SEVERA', 'TEMPORAL CENTRAL MUNICIPALIDAD': 'TORMENTA SEVERA',

            # SEQUIA
            'SEQ. E INUND.': 'SEQUIA', 'SEQ./INUND.': 'SEQUIA', 'SEQUIA-INUND.': 'SEQUIA',

            # EXTREMA VULNERABILIDAD
            'COMISION VECINAL': 'EXTREMA VULNERABILIDAD',
            'AYUDA SOLIDARIA': 'EXTREMA VULNERABILIDAD',

            # C.I.D.H.
            'C I D H': 'C.I.D.H.', 'C.H.D.H': 'C.I.D.H.', 'C.I.D.H': 'C.I.D.H.',
            'C.I.D.H.': 'C.I.D.H.', 'C.ID.H': 'C.I.D.H.', 'CIDH': 'C.I.D.H.',

            # OPERATIVO JAHO'I
            'OPERATIVO ÑEÑUA': "OPERATIVO JAHO'I", 'OPERATIVO ESPECIAL': "OPERATIVO JAHO'I",
            'OP INVIERNO': "OPERATIVO JAHO'I", 'OP. INVIERNO': "OPERATIVO JAHO'I",
            'OP. ÑEÑUA': "OPERATIVO JAHO'I", 'OP.INVIERNO': "OPERATIVO JAHO'I",

            # INUNDACION
            'INUNDAC.': 'INUNDACION', 'INUNDAIÓN S.': 'INUNDACION',
            'INUNDACION SUBITA': 'INUNDACION', 'INUNDACION " DECLARACION DE EMERGENCIA"': 'INUNDACION',
            'LNUNDACION': 'INUNDACION', 'INUNDACIÓN': 'INUNDACION',

            # OLLA POPULAR
            'OLLA P': 'OLLA POPULAR', 'OLLA P.': 'OLLA POPULAR', 'OLLA POP': 'OLLA POPULAR',
            'OLLA POP.': 'OLLA POPULAR', 'OLLA POPILAR': 'OLLA POPULAR',
            'OLLA POPOLAR': 'OLLA POPULAR', 'OLLA POPUL': 'OLLA POPULAR',
            'OLLAP.': 'OLLA POPULAR', 'OLLA POPULAR COVID': 'OLLA POPULAR',

            # OTROS
            'INERAM': 'OTROS', 'INERAM(MINGA)': 'OTROS', 'MINGA': 'OTROS',
            'INDERT': 'OTROS', 'INDI MBYA GUARANI': 'OTROS', 'NIÑEZ': 'OTROS',
            'DGRR 027/22': 'OTROS', 'DGRR 028/22': 'OTROS', 'DONAC': 'OTROS',
            'DONAC.': 'OTROS', 'DONACIÒN': 'OTROS', 'EDAN': 'OTROS',
            'EVALUACION DE DAÑOS': 'OTROS', 'TRABAJO COMUNITARIO': 'OTROS',
            'ASISTENCIA INSTITUCIONAL': 'OTROS', 'APOYO LOGISTICO': 'OTROS',
            'APOYO INSTITUCIONAL': 'OTROS', 'APOY.LOG': 'OTROS', 'APOY LOG': 'OTROS',
            'APOYO LOG.': 'OTROS', 'OTROS "TEMPORAL"': 'OTROS',
            'APOYO LOGISTICO INDI': 'OTROS',

            # PREPOSICIONAMIENTO (ELIMINAR)
            'PREP.': 'ELIMINAR_REGISTRO', 'PREPOS': 'ELIMINAR_REGISTRO',
            'PREPOS.': 'ELIMINAR_REGISTRO', 'PREPOSIC.': 'ELIMINAR_REGISTRO',
            'PREPOSICION.': 'ELIMINAR_REGISTRO', 'PRE POSICIONAMIENTO': 'ELIMINAR_REGISTRO',
            'P/ STOCK DEL COE': 'ELIMINAR_REGISTRO', 'REP.DE MATERIAL': 'ELIMINAR_REGISTRO',
            'REPOSIC.MATER': 'ELIMINAR_REGISTRO', 'REPOSIC.MATER.': 'ELIMINAR_REGISTRO',
            'PROVISION DE MATERIALES': 'ELIMINAR_REGISTRO', 'REABASTECIMIENTO': 'ELIMINAR_REGISTRO',
            'REPARACION': 'ELIMINAR_REGISTRO', 'REPARACION DE BAÑADERA': 'ELIMINAR_REGISTRO',
            'REPARACION DE OBRES': 'ELIMINAR_REGISTRO', 'PRESTAMO': 'ELIMINAR_REGISTRO',
            'REPOSICION': 'ELIMINAR_REGISTRO', 'REPOSICION DE MATERIALES': 'ELIMINAR_REGISTRO',
            'TRASLADO INTERNO': 'ELIMINAR_REGISTRO', 'PREPOSICIONAMIENTO': 'ELIMINAR_REGISTRO',

            # SIN EVENTO
            'SIN_EVENTO': 'SIN EVENTO', 'DEVOLVIO': 'SIN EVENTO',
            'REFUGIO SEN': 'SIN EVENTO', '': 'SIN EVENTO',
            'SIN EVENTO': 'SIN EVENTO'
        }

    def limpiar_texto(self, texto):
        """Normaliza un campo de texto: trim, mayúsculas y valor por defecto.

        Devuelve 'SIN ESPECIFICAR' si el valor es nulo o vacío.
        """
        if pd.isna(texto) or texto is None or str(texto).strip() == '':
            return 'SIN ESPECIFICAR'
        return str(texto).strip().upper()

    def limpiar_numero(self, value):
        """Convierte a entero de forma segura.

        Acepta strings con coma o punto decimales. Devuelve 0 si no se puede
        parsear.
        """
        try:
            # Aceptar cadenas como '1,5' o '1.5'
            if isinstance(value, str):
                value = value.replace(',', '.')
            return int(float(value)) if value not in [None, '', np.nan] else 0
        except (ValueError, TypeError):
            return 0

    def estandarizar_departamento_robusto(self, departamento):
        """Normaliza el nombre de departamento con varias heurísticas.

        Reglas aplicadas, en orden:
        1) Lookup directo en el diccionario de correcciones.
        2) Si contiene separadores, toma la primera parte y vuelve a buscar.
        3) Busca coincidencias parciales con los nombres válidos.
        4) Si no se identifica, devuelve 'CENTRAL' por defecto.
        """
        if pd.isna(departamento) or departamento is None:
            return 'CENTRAL'

        depto_limpio = self.limpiar_texto(departamento)

        # 1. Búsqueda directa en el diccionario
        if depto_limpio in self.estandarizacion_dept:
            return self.estandarizacion_dept[depto_limpio]

        # 2. Si contiene separadores, probar con la primera parte
        for sep in [' - ', ' / ', ', ', ' Y ']:
            if sep in depto_limpio:
                primera_parte = depto_limpio.split(sep)[0].strip()
                if primera_parte in self.estandarizacion_dept:
                    return self.estandarizacion_dept[primera_parte]

        # 3. Coincidencia parcial con nombres válidos
        for depto_estandar in self.departamento_orden.keys():
            if depto_estandar in depto_limpio:
                return depto_estandar

        # 4. Fallback por defecto
        return 'CENTRAL'


    def estandarizar_evento_robusto(self, evento):
        """Normaliza el campo 'EVENTO'.

        Intenta un lookup directo y, si falla, busca palabras clave que indiquen
        la categoría. Si no encuentra nada, devuelve 'SIN EVENTO'.
        """
        if pd.isna(evento) or evento is None:
            return 'SIN EVENTO'

        evento_limpio = self.limpiar_texto(evento)

        # 1. Lookup directo
        if evento_limpio in self.estandarizacion_eventos:
            return self.estandarizacion_eventos[evento_limpio]

        # 2. Búsqueda por palabras clave (heurística rápida)
        palabras_clave = {
            'COVID': 'COVID', 'INCENDIO': 'INCENDIO', 'TORMENTA': 'TORMENTA SEVERA',
            'TEMPORAL': 'TORMENTA SEVERA', 'INUNDACION': 'INUNDACION',
            'SEQUIA': 'SEQUIA', 'JAHO': "OPERATIVO JAHO'I", 'ÑEÑUA': "OPERATIVO JAHO'I",
            'OLLA': 'OLLA POPULAR', 'VULNERABILIDAD': 'EXTREMA VULNERABILIDAD',
            'CIDH': 'C.I.D.H.'
        }

        for palabra, evento_estandar in palabras_clave.items():
            if palabra in evento_limpio:
                return evento_estandar

        # 3. Sin coincidencias: marcar como sin evento
        return 'SIN EVENTO'


    def post_process_eventos_with_aids(self, row):
        """Inferencia de evento a partir de insumos y contexto de la fila.

        Aplica reglas heurísticas (kits, chapas, departamento) para inferir
        un 'EVENTO' cuando no viene especificado. Devuelve 'ELIMINAR_REGISTRO'
        para registros que deben descartarse (p. ej. preposicionamiento).
        """
        evento = row.get('EVENTO', 'SIN EVENTO')

        # Si es preposicionamiento, lo eliminamos
        if evento == 'ELIMINAR_REGISTRO':
            return 'ELIMINAR_REGISTRO'

        # Si no tiene evento, aplicamos las reglas enriquecidas
        if evento == 'SIN EVENTO' or evento == '' or evento is None:
            # Asegurar que DEPARTAMENTO está en mayúsculas para la comparación
            departamento = str(row.get('DEPARTAMENTO', '')).upper()

            # Regla 1: departamentos secos -> SEQUIA
            if departamento in ['BOQUERON', 'ALTO PARAGUAY', 'PDTE. HAYES']:
                return 'SEQUIA'

            # Obtener valores de kits y materiales (usando limpieza robusta)
            kit_b = self.limpiar_numero(row.get('KIT B', row.get('KIT_B', 0)))
            kit_a = self.limpiar_numero(row.get('KIT A', row.get('KIT_A', 0)))
            total_kits = kit_b + kit_a

            chapa_zinc = self.limpiar_numero(row.get('CHAPA ZINC', row.get('CHAPA_ZINC', 0)))
            chapa_fibrocemento = self.limpiar_numero(row.get('CHAPA FIBROCEMENTO', row.get('CHAPA_FIBROCEMENTO', 0)))

            # Suma de materiales para Regla 4 y 5
            materiales_cols = [
                'CHAPA FIBROCEMENTO', 'CHAPA_FIBROCEMENTO', 'CHAPA ZINC', 'CHAPA_ZINC',
                'COLCHONES', 'FRAZADAS', 'TERCIADAS', 'PUNTALES', 'CARPAS PLASTICAS', 'CARPAS_PLASTICAS'
            ]
            
            # Nota: Usamos la suma total de *todos* los insumos, incluyendo kits y materiales para la lógica
            # Esto es clave para las reglas de inferencia.
            materiales_no_kits_cols = [
                'CHAPA FIBROCEMENTO', 'CHAPA_FIBROCEMENTO', 'CHAPA ZINC', 'CHAPA_ZINC',
                'COLCHONES', 'FRAZADAS', 'TERCIADAS', 'PUNTALES', 'CARPAS PLASTICAS', 'CARPAS_PLASTICAS'
            ]
            materiales = sum(self.limpiar_numero(row.get(field, 0)) for field in materiales_no_kits_cols)
            total_insumos = total_kits + materiales

            # Regla 2: pocos kits pero hay materiales -> INCENDIO
            # (0 < total_kits < 10 y materiales > 0)
            if total_kits < 10 and total_kits > 0 and materiales > 0:
                return 'INCENDIO'

            # Regla 4: en CAPITAL, solo kits (sin materiales) -> INUNDACION
            if departamento == 'CAPITAL' and total_kits > 0 and materiales == 0:
                return 'INUNDACION'

            # Regla 5: solo chapa_zinc presente -> TORMENTA SEVERA
            # La condición `materiales == chapa_zinc` asegura que solo hay ese material.
            if chapa_zinc > 0 and total_kits == 0 and chapa_fibrocemento == 0 and materiales == chapa_zinc:
                return 'TORMENTA SEVERA'

            # Regla 6: solo chapa_fibrocemento presente -> INUNDACION
            # La condición `materiales == chapa_fibrocemento` asegura que solo hay ese material.
            if chapa_fibrocemento > 0 and total_kits == 0 and chapa_zinc == 0 and materiales == chapa_fibrocemento:
                return 'INUNDACION'

            # Regla 7: si hay kits -> EXTREMA VULNERABILIDAD
            if total_kits > 0:
                return 'EXTREMA VULNERABILIDAD'
            
            # Si llegó aquí, no tenía evento, no cumplió ninguna regla de inferencia
            # y no tenía kits, ni materiales, o solo tenía materiales pero no Kits/Chapas específicas.
            # En el script original, el valor final para estos casos sin evento/insumos es 'EXTREMA VULNERABILIDAD'.
            # Sin embargo, la lógica de negocio exige que si no hay insumos se elimine.
            # Aquí lo marcamos como 'SIN_INSUMOS' para el paso de eliminación final.
            if total_insumos == 0:
                return 'SIN_INSUMOS'

            return 'EXTREMA VULNERABILIDAD'

        return evento

    def run_complete_correction_pipeline(self, df):
        """Ejecuta todo el pipeline de corrección sobre un DataFrame.

        Pasos principales:
        1) Normaliza nombres de columnas.
        2) Estandariza departamentos y eventos.
        3) Infere eventos a partir de insumos y limpia registros inválidos.
        4) Genera features básicos y asegura el esquema final.
        """
        print("🎯 Aplicando estandarización robusta de DEPARTAMENTO y EVENTO...")

        # Normalizar nombres de columnas a MAYÚSCULAS con guiones bajos
        df.columns = [col.upper().replace(' ', '_') for col in df.columns]

        # 1. Estandarizar departamentos
        if 'DEPARTAMENTO' in df.columns:
            df['DEPARTAMENTO'] = df['DEPARTAMENTO'].apply(self.estandarizar_departamento_robusto)

        # 2. Estandarizar eventos (antes de la inferencia)
        if 'EVENTO' in df.columns:
            df['EVENTO'] = df['EVENTO'].apply(self.estandarizar_evento_robusto)

        # 3. Inferir eventos y limpiar filas según reglas de insumos
        print("🔍 Aplicando inferencia de eventos basada en recursos...")
        eventos_inferidos = 0
        
    # Preparar columnas numéricas temporales para evitar parseos repetidos
        
    # Lista de columnas de insumos que se usarán en la inferencia
        insumos_cols_map = {
            'KIT_A': 'KIT_A', 'KIT_B': 'KIT_B',
            'CHAPA_FIBROCEMENTO': 'CHAPA_FIBROCEMENTO', 'CHAPA_ZINC': 'CHAPA_ZINC',
            'COLCHONES': 'COLCHONES', 'FRAZADAS': 'FRAZADAS', 
            'TERCIADAS': 'TERCIADAS', 'PUNTALES': 'PUNTALES', 'CARPAS_PLASTICAS': 'CARPAS_PLASTICAS'
        }
        
        # Mapear las columnas originales a los nombres estandarizados
        temp_col_map = {}
        for final_col, _ in insumos_cols_map.items():
            # Buscar la columna en las columnas del DF
            found_col = next((col for col in df.columns if col == final_col), None)
            if found_col:
                df[f'{final_col}_TEMP'] = df[found_col].apply(self.limpiar_numero)
                temp_col_map[final_col] = f'{final_col}_TEMP'
            else:
                # Si no existe, crearla como 0 para el cálculo
                df[f'{final_col}_TEMP'] = 0
                temp_col_map[final_col] = f'{final_col}_TEMP'


    # Re-iterar por fila aplicando la lógica de inferencia (usa las columnas _TEMP)
        for idx, row in df.iterrows():
            # Construir un dict temporal con las columnas clave para el post-procesamiento
            temp_row = row.to_dict()
            for final_col, temp_col in temp_col_map.items():
                temp_row[final_col.replace('_', ' ')] = row[temp_col] # Necesario para la función
            
            # Pasar la fila con las columnas limpias a la función
            evento_original = row['EVENTO']
            evento_inferido = self.post_process_eventos_with_aids(temp_row)
            
            if evento_original != evento_inferido:
                eventos_inferidos += 1
                df.at[idx, 'EVENTO'] = evento_inferido
        
        # Eliminar columnas temporales
        cols_to_drop = [f'{col}_TEMP' for col in insumos_cols_map.keys() if f'{col}_TEMP' in df.columns]
        df = df.drop(columns=cols_to_drop, errors='ignore')

        # 4. Eliminar registros marcados (preposicionamiento) y aquellos sin insumos
        registros_antes = len(df)

        # Realizar la limpieza de números en las columnas de insumos para el cálculo total
        for col in insumos_cols_map.keys():
            df[col] = df.get(col, pd.Series([0] * len(df), index=df.index)).apply(self.limpiar_numero)

        # Calcular el total de insumos
        insumo_cols = list(insumos_cols_map.keys())
        df['TOTAL_INSUMOS'] = df[insumo_cols].sum(axis=1)

        # 4a. Eliminar ELIMINAR_REGISTRO (Preposicionamiento)
        df_limpio = df[df['EVENTO'] != 'ELIMINAR_REGISTRO'].copy()
        registros_eliminados_prepos = len(df) - len(df_limpio)

        # 4b. Eliminar registros sin insumos
        df_limpio = df_limpio[df_limpio['TOTAL_INSUMOS'] > 0]
        registros_eliminados_cero = registros_antes - registros_eliminados_prepos - len(df_limpio)

        df = df_limpio.drop(columns=['TOTAL_INSUMOS'], errors='ignore')

        print(f"   Registros eliminados (Preposicionamiento): {registros_eliminados_prepos:,}")
        print(f"   Registros eliminados (Sin insumos): {registros_eliminados_cero:,}")
        print(f"   Registros restantes: {len(df):,}")

        # 5. Generar columnas derivadas (AÑO, MES, ORDEN_DEPARTAMENTO, ...)
        df = self.feature_engineering_basico(df)

        # 6. Asegurar esquema y tipos para la carga en el DW
        df = self.estandarizacion_final_columnas(df)

        return df

    # ... (feature_engineering_basico, estandarizacion_final_columnas y verificacion_final se mantienen iguales)
    def feature_engineering_basico(self, df):
        """Feature engineering básico (Alineado con tu archivo original)"""
        # print("⚙️ Aplicando feature engineering...")

        fecha_cols = [col for col in df.columns if 'FECHA' in col.upper()]
        if fecha_cols:
            col_fecha = fecha_cols[0]
            df[col_fecha] = pd.to_datetime(df[col_fecha], errors='coerce')
            df['AÑO'] = df[col_fecha].dt.year
            df['MES'] = df[col_fecha].dt.month

        # Agregar orden de departamento
        if 'DEPARTAMENTO' in df.columns:
            # Asegura que solo los 18 departamentos válidos tengan orden
            df['ORDEN_DEPARTAMENTO'] = df['DEPARTAMENTO'].map(self.departamento_orden).fillna(0).astype(int)

        return df

    def estandarizacion_final_columnas(self, df):
        """
        Asegura que solo las columnas necesarias para el DW estén presentes
        y con el nombre y tipo correcto.
        """
        # Columnas finales esperadas
        columnas_finales = {
            'FECHA': 'datetime64[ns]',
            'LOCALIDAD': 'object',
            'DISTRITO': 'object',
            'DEPARTAMENTO': 'object',
            'EVENTO': 'object',
            'KIT_B': 'int64',
            'KIT_A': 'int64',
            'CHAPA_FIBROCEMENTO': 'int64',
            'CHAPA_ZINC': 'int64',
            'COLCHONES': 'int64',
            'FRAZADAS': 'int64',
            'TERCIADAS': 'int64',
            'PUNTALES': 'int64',
            'CARPAS_PLASTICAS': 'int64',
            'AÑO': 'int64',
            'MES': 'int64',
            'ORDEN_DEPARTAMENTO': 'int64'
        }

        df_final = pd.DataFrame()
        for col, dtype in columnas_finales.items():
            # Crear la columna si no existe (con valor 0 o SIN ESPECIFICAR)
            if col not in df.columns:
                if dtype in ['int64', 'float64']:
                    df_final[col] = 0
                else:
                    df_final[col] = 'SIN ESPECIFICAR'
            else:
                # Copiar y limpiar/convertir
                if dtype in ['int64', 'float64']:
                    # Limpieza explícita para asegurar que sean números
                    df_final[col] = df[col].apply(self.limpiar_numero)
                elif dtype == 'object':
                    df_final[col] = df[col].apply(self.limpiar_texto)
                elif dtype == 'datetime64[ns]':
                    df_final[col] = pd.to_datetime(df[col], errors='coerce')

        return df_final.astype(columnas_finales, errors='ignore')

    def verificacion_final(self, df):
        """Verificación final de la estandarización"""
        print("\n🔍 VERIFICACIÓN FINAL:")

        if 'DEPARTAMENTO' in df.columns:
            deptos_finales = df['DEPARTAMENTO'].unique()
            deptos_esperados = set(self.departamento_orden.keys())
            deptos_extra = set(deptos_finales) - deptos_esperados
            
            # Quitar 'SIN ESPECIFICAR' y 'CENTRAL' si están presentes en deptos_finales y no en esperados
            deptos_finales_limpios = {d for d in deptos_finales if d in deptos_esperados}
            deptos_extra = set(deptos_finales) - deptos_esperados
            
            print(f"✅ DEPARTAMENTOS FINALES: {len(deptos_finales)}")
            print(f"📋 Lista: {sorted(list(deptos_finales))}")

            if deptos_extra:
                print(f"❌ DEPARTAMENTOS EXTRA/NO ESPERADOS: {deptos_extra}")
            else:
                print("🎉 ¡Todos los departamentos están correctamente estandarizados o son esperados!")

        if 'EVENTO' in df.columns:
            eventos_finales = df['EVENTO'].value_counts()
            print(f"\n✅ EVENTOS FINALES: {len(eventos_finales)}")
            print("📊 Distribución Top 10:")
            for evento, count in eventos_finales.head(10).items():
                print(f"   - {evento}: {count}")

        return df # Devuelve el DF para encadenamiento si es necesario