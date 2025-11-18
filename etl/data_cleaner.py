# -*- coding: utf-8 -*-
"""Pipeline de limpieza y estandarización para el ETL - Fase de Transformación.

Este módulo se encarga de preparar los datos crudos para que estén listos 
para ser cargados en el Data Warehouse. Piensa en esto como la "cocina" donde 
los datos crudos se convierten en información lista para analizar.

Trabajamos principalmente en tres áreas:
1. Ubicaciones: Para que todos los departamentos, distritos y localidades 
   sigan un formato estándar
2. Eventos: Para categorizar consistentemente los tipos de emergencias y ayudas
3. Cantidades: Para asegurar que todos los números estén en formato correcto
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
import re
import unicodedata
import json
from pathlib import Path
from difflib import SequenceMatcher
import Levenshtein
warnings.filterwarnings('ignore')


class DataCleaner:
    def __init__(self):
        """Prepara todas las herramientas y reglas para limpiar los datos.
        
        Aquí definimos todos los diccionarios y reglas que usaremos para 
        estandarizar la información. Es como tener un manual de instrucciones 
        para transformar datos inconsistentes en información confiable.
        """
        
        # Campos históricos que pueden aparecer en versiones antiguas de los datos
        self.original_kit_fields = ['kit_a', 'kit_b']

        # Tipos de ayuda humanitaria que el sistema debe reconocer
        self.aid_fields = [
            'kit_eventos', 'kit_sentencia', 'chapa_fibrocemento', 'chapa_zinc',
            'colchones', 'frazadas', 'terciadas', 'puntales', 'carpas_plasticas'
        ]

        # Orden oficial de los departamentos para reportes y dashboards
        # Esto asegura que en los reportes los departamentos aparezcan siempre en el mismo orden
        self.departamento_orden = {
            'CONCEPCIÓN': 1, 'SAN PEDRO': 2, 'CORDILLERA': 3, 'GUAIRÁ': 4,
            'CAAGUAZÚ': 5, 'CAAZAPÁ': 6, 'ITAPÚA': 7, 'MISIONES': 8,
            'PARAGUARÍ': 9, 'ALTO PARANÁ': 10, 'CENTRAL': 11, 'ÑEEMBUCÚ': 12,
            'AMAMBAY': 13, 'CANINDEYÚ': 14, 'PDTE. HAYES': 15, 'BOQUERON': 16,
            'ALTO PARAGUAY': 17, 'CAPITAL': 18
        }

        # Diccionario para corregir nombres de departamentos
        # Aquí mapeamos todas las formas en que la gente escribe los departamentos
        # hacia la forma oficial que usaremos en el sistema
        self.estandarizacion_dept = {
            # Correcciones para Ñeembucú
            'ÑEEMBUCU': 'ÑEEMBUCÚ', 'ÑEEMBUCÙ': 'ÑEEMBUCÚ', 'ÑEMBUCU': 'ÑEEMBUCÚ',
            'Ñeembucu': 'ÑEEMBUCÚ', 'ÑEEMBUCÚ': 'ÑEEMBUCÚ',

            # Correcciones para Alto Paraná
            'ALTO PARANA': 'ALTO PARANÁ', 'ALTO PARANÀ': 'ALTO PARANÁ',
            'ALTO PNÀ': 'ALTO PARANÁ', 'ALTO PNÁ': 'ALTO PARANÁ', 'ALTO PY': 'ALTO PARANÁ',
            'Alto Parana': 'ALTO PARANÁ', 'ALTO PARANÁ': 'ALTO PARANÁ',

            # Correcciones para Boquerón
            'BOQUERÒN': 'BOQUERON', 'BOQUERÓN': 'BOQUERON', 'Boqueron': 'BOQUERON',
            'BOQUERON': 'BOQUERON',

            # Correcciones para Caaguazú (uno de los que más variaciones tiene)
            'CAAGUAZU': 'CAAGUAZÚ', 'CAAGUAZÙ': 'CAAGUAZÚ', 'Caaguazu': 'CAAGUAZÚ',
            'Caaguazú': 'CAAGUAZÚ', 'CAAGUAZÚ': 'CAAGUAZÚ',
            'CAAG-CANIND': 'CAAGUAZÚ', 'CAAG/CANIN': 'CAAGUAZÚ', 'CAAG/CANIND.': 'CAAGUAZÚ',
            'CAAGUAZU- ALTO PARANA': 'CAAGUAZÚ', 'CAAGUAZU/MISIONES': 'CAAGUAZÚ',
            'CAAGUAZU - CANINDEYU': 'CAAGUAZÚ', 'CAAGUAZU Y CANINDEYU': 'CAAGUAZÚ',
            'CAAGUAZU, CANINDEYU Y SAN PEDRO': 'CAAGUAZÚ',
            'CAAGUAZU, SAN PEDRO Y CANINDEYU': 'CAAGUAZÚ',
            'CAAGUAZU-GUAIRA Y SAN PEDRO': 'CAAGUAZÚ', 'CAAGUAZU-GUAIRA': 'CAAGUAZÚ',

            # Correcciones para Caazapá
            'CAAZAPA': 'CAAZAPÁ', 'CAAZAPÀ': 'CAAZAPÁ', 'Caazapa': 'CAAZAPÁ',
            'CAAZAPÁ': 'CAAZAPÁ', 'CAAZAPA - GUAIRA': 'CAAZAPÁ',
            'Caazapa - Guaira': 'CAAZAPÁ',

            # Correcciones para Canindeyú
            'CANINDEYU': 'CANINDEYÚ', 'CANINDEYÙ': 'CANINDEYÚ', 'Canindeyu': 'CANINDEYÚ',
            'CANINDEYÚ': 'CANINDEYÚ', 'CANINDEYU - CAAGUAZU': 'CANINDEYÚ',
            'CANINDEYU Y SAN PEDRO': 'CANINDEYÚ',

            # Correcciones para Central
            'CENT/CORDILL': 'CENTRAL', 'CENTR-CORD': 'CENTRAL', 'CENTRAL': 'CENTRAL',
            'CENTRAL-CORDILLERA': 'CENTRAL', 'CENTRAL/CAP': 'CENTRAL', 'CENTRAL/CAPITAL': 'CENTRAL',
            'CENTRAL/COR': 'CENTRAL', 'CENTRAL/CORD': 'CENTRAL', 'CENTRAL/CORD.': 'CENTRAL',
            'CENTRAL/CORDILLER': 'CENTRAL', 'CENTRAL/CORDILLERA': 'CENTRAL',
            'CENTRAL/PARAG.': 'CENTRAL', 'central': 'CENTRAL',

            # Correcciones para Concepción
            'CONCEPCION': 'CONCEPCIÓN', 'CONCEPCIÒN': 'CONCEPCIÓN', 'Concepcion': 'CONCEPCIÓN',
            'CONCEPCIÓN': 'CONCEPCIÓN',

            # Correcciones para Cordillera
            'COORDILLERA': 'CORDILLERA', 'CORD./CENTRAL': 'CORDILLERA',
            'CORD/S.PEDRO': 'CORDILLERA', 'CORDILLERA': 'CORDILLERA',
            'CORDILLERA ARROYOS Y EST.': 'CORDILLERA', 'CORDILLERA Y SAN PEDRO': 'CORDILLERA',
            'CORDILLERACAACUPÈ': 'CORDILLERA', 'Cordillera': 'CORDILLERA',
            'CORDILLERA ARROYOS': 'CORDILLERA',

            # Correcciones para Guairá
            'GUAIRA': 'GUAIRÁ', 'GUAIRÀ': 'GUAIRÁ', 'GUIARA': 'GUAIRÁ',
            'Guaira': 'GUAIRÁ', 'GUAIRÁ': 'GUAIRÁ',
            'GUAIRA - CAAZAPA': 'GUAIRÁ', 'Guaira - Caazapa': 'GUAIRÁ',

            # Correcciones para Itapúa
            'ITAPUA': 'ITAPÚA', 'ITAPUA- CAAGUAZU': 'ITAPÚA', 'ITAPÙA': 'ITAPÚA',
            'Itapua': 'ITAPÚA', 'ITAPÚA': 'ITAPÚA',

            # Correcciones para Misiones
            'MISIONES YABEBYRY': 'MISIONES', 'Misiones': 'MISIONES', 'MISIONES': 'MISIONES',

            # Correcciones para Paraguarí
            'PARAGUARI': 'PARAGUARÍ', 'PARAGUARI PARAGUARI': 'PARAGUARÍ',
            'PARAGUARÌ': 'PARAGUARÍ', 'Paraguari': 'PARAGUARÍ', 'PARAGUARÍ': 'PARAGUARÍ',
            'PARAGUARI - GUAIRA': 'PARAGUARÍ', 'Paraguari - Guaira': 'PARAGUARÍ',
            'PARAGUARI - GUAIRA': 'PARAGUARÍ',

            # Correcciones para Pte. Hayes
            'PDTE HAYES': 'PDTE. HAYES', 'PDTE HAYES S.PIRI-4 DE MAYO': 'PDTE. HAYES',
            'PDTE HYES': 'PDTE. HAYES', 'PDTE. HAYES': 'PDTE. HAYES', 'PTE HAYES': 'PDTE. HAYES',
            'PTE. HAYES': 'PDTE. HAYES', 'Pdte Hayes': 'PDTE. HAYES', 'Pdte. Hayes': 'PDTE. HAYES',
            'PDTE.HAYES': 'PDTE. HAYES',

            # Correcciones para San Pedro
            'S.PEDRO/CAN.': 'SAN PEDRO', 'SAN PEDRO': 'SAN PEDRO',
            'SAN PEDRO-CAAGUAZU': 'SAN PEDRO', 'SAN PEDRO/ AMAMBAY': 'SAN PEDRO',
            'SAN PEDRO/ CANINDEYU': 'SAN PEDRO', 'San Pedro': 'SAN PEDRO',
            'SAN PEDRO - CANINDEYU': 'SAN PEDRO', 'San Pedro - Canindeyu': 'SAN PEDRO',

            # Casos especiales - cuando no está claro el departamento, usamos Central por defecto
            'VARIOS DEP.': 'CENTRAL', 'VARIOS DPTOS.': 'CENTRAL', 'VARIOS DPTS.': 'CENTRAL',
            'varios': 'CENTRAL', 'REGION ORIENTAL/ OCCIDENTAL': 'CENTRAL',
            'VARIOS': 'CENTRAL', 'ASOC MUSICO': 'CENTRAL', 'INDI': 'CENTRAL',
            'SIN_DEPARTAMENTO': 'CENTRAL', 'SIN ESPECIFICAR': 'CENTRAL',

            # Cuando la gente escribe el nombre del distrito en lugar del departamento
            'CNEL OVIEDO': 'CAAGUAZÚ', 'ITA': 'CENTRAL', 'ITAUGUA': 'CENTRAL',
            'VILLARICA': 'GUAIRÁ', 'ASUNCION': 'CAPITAL', 'ASUNCIÓN': 'CAPITAL',
            'CAACUPÈ': 'CORDILLERA', 'CAACUPÉ': 'CORDILLERA',

            # Departamentos que ya vienen correctos
            'ALTO PARAGUAY': 'ALTO PARAGUAY', 'AMAMBAY': 'AMAMBAY', 'CAPITAL': 'CAPITAL'
        }

        # Lista oficial de todos los distritos válidos organizados por departamento
        # Esto nos sirve para validar que los distritos existen y pertenecen al departamento correcto
        self.DISTRITOS_POR_DEPARTAMENTO = {
            'CAPITAL': {'ASUNCIÓN'},
            'CONCEPCIÓN': {'CONCEPCIÓN', 'BELÉN', 'HORQUETA', 'LORETO', 'SAN CARLOS', 'SAN LÁZARO', 'YVY YA\'Ú', 'AZOTEY', 'SGTO. JOSÉ FÉLIX LÓPEZ', 'SAN ALFREDO', 'PASO BARRETO', 'ITACUÁ', 'PASO HORQUETA', 'ARROYITO'},
            'SAN PEDRO': {'SAN PEDRO DEL YKUAMANDIYÚ', 'ANTEQUERA', 'CHORE', 'GRAL. ELIZARDO AQUINO', 'ITACURUBÍ DEL ROSARIO', 'LIMA', 'NUEVA GERMANIA', 'SAN ESTANISLAO', 'SAN PABLO', 'TACUATÍ', 'UNIÓN', '25 DE DICIEMBRE', 'VILLA DEL ROSARIO', 'GRAL. RESQUÍN', 'YATAITY DEL NORTE', 'GUAJAYVÍ', 'CAPIIBARY', 'SANTA ROSA DEL AGUARAY', 'YRYBUCUÁ', 'LIBERACIÓN', 'SAN JOSÉ DEL ROSARIO', 'SAN VICENTE PANCHOLO'},
            'CORDILLERA': {'CAACUPÉ', 'ALTOS', 'ARROYOS Y ESTEROS', 'ATYRÁ', 'CARAGUATAY', 'EMBOSCADA', 'EUSEBIO AYALA', 'ISLA PUCÚ', 'ITACURUBÍ DE LA CORDILLERA', 'JUAN DE MENA', 'LOMA GRANDE', 'MBOCAYATY DEL YHAGUY', 'NUEVA COLOMBIA', 'PIRIBEBUY', 'PRIMERO DE MARZO', 'SAN BERNARDINO', 'SANTA ELENA', 'SAN JOSÉ OBRERO', 'TOBATÍ', 'VALENZUELA'},
            'GUAIRÁ': {'VILLARRICA', 'BORJA', 'CAPITÁN MAURICIO JOSÉ TROCHE', 'COLONIA INDEPENDENCIA', 'DR. BOTRELL', 'FÉLIX PÉREZ CARDOZO', 'GARAMBARÉ', 'ITAPÉ', 'ITURBE', 'JOSÉ FASSARDI', 'MBOCAYATY', 'NATALICIO TALAVERA', 'ÑUMÍ', 'PASO YOBÁI', 'SAN SALVADOR', 'TEBAICUARY', 'YATAITY DEL GUAIRÁ'},
            'CAAGUAZÚ': {'CAAGUAZÚ', 'CORONEL OVIEDO', 'DR. J. EULOGIO ESTIGARRIBIA', 'DR. JUAN MANUEL FRUTOS', 'SAN JOAQUÍN', 'SAN JOSÉ DE LOS ARROYOS', 'LA PASTORA', 'NUEVA TOLEDO', 'VAQUERÍA', 'YHÚ', '3 DE FEBRERO', 'CARAYAÓ', 'MCAL. FCO. SOLANO LÓPEZ', 'NUEVA LONDRES', 'RAÚL ARSENIO OVIEDO', 'REPATRIACIÓN', 'R.I. 3 CORRALES', 'SIMÓN BOLIVAR'},
            'CAAZAPÁ': {'CAAZAPÁ', '3 DE MAYO', 'ABAÍ', 'BUENA VISTA', 'CORONEL MACIEL', 'DR. MOISÉS S. BERTONI', 'FULGENCIO YEGROS', 'GRAL. HIGINIO MORÍNIGO', 'SAN JUAN NEPOMUCENO', 'TAVAÍ', 'YUTY'},
            'ITAPÚA': {'ENCARNACIÓN', 'CAMBYRETÁ', 'CAPITÁN MIRANDA', 'CORONEL BOGADO', 'FRAM', 'GENERAL ARTIGAS', 'HOHENAU', 'JESÚS DE TAVARANGUÉ', 'LA PAZ', 'OBLIGADO', 'PIRAPÓ', 'SAN COSME Y DAMIÁN', 'SAN JUAN DEL PARANÁ', 'SAN PEDRO DEL PARANÁ', 'TRINIDAD', 'NATALIO', 'CARMEN DEL PARANÁ', 'YATYTAY', 'MAYOR JOSÉ D. OTAÑO', 'SAN RAFAEL DEL PARANÁ', 'BELLA VISTA', 'CAPITÁN MEZA', 'EDELIRA', 'ITAPÚA POTY', 'ALTO VERÁ', 'TOMÁS ROMERO PEREIRA'},
            'MISIONES': {'SAN JUAN BAUTISTA', 'AYOLAS', 'SAN IGNACIO GUAZÚ', 'SANTA MARÍA DE FE', 'SANTIAGO', 'VILLA FLORIDA', 'YABEBIRY', 'SAN MIGUEL', 'SAN PATRICIO', 'SANTA ROSA'},
            'PARAGUARÍ': {'PARAGUARÍ', 'CARAPEGUÁ', 'ACAHAY', 'CAAPUCÚ', 'CABALLERO', 'ESCOBER', 'LA COLMENA', 'MBUYAPEY', 'PIRAYÚ', 'QUIINDY', 'QUYQUYHÓ', 'SAN ROQUE GONZÁLEZ DE SANTA CRUZ', 'SAPUCAI', 'TEBICUARYMÍ', 'YAGUARÓN', 'YBYCUÍ', 'YBYTYMÍ', 'MARÍA ANTONIA'},
            'ALTO PARANÁ': {'CIUDAD DEL ESTE', 'DOCTOR JUAN LEÓN MALLORQUÍN', 'DOCTOR RAÚL PEÑA',       'DOMINGO MARTÍNEZ DE IRALA', 'HERNANDARIAS', 'IRUÑA', 'ITAKYRY', 'JUAN EMILIO O\'LEARY', 'LOS CEDRALES', 'MBARACAYÚ', 'MINGA GUAZÚ', 'MINGA PORÁ', 'NARANJAL', 'ÑACUNDAY', 'PRESIDENTE FRANCO', 'SAN ALBERTO', 'SAN CRISTÓBAL', 'SANTA FE DEL PARANÁ', 'SANTA RITA', 'SANTA ROSA DEL MONDAY', 'TAVAPY', 'YGUAZÚ'},
            'CENTRAL': {'AREGUÁ', 'CAPIATÁ', 'FERNANDO DE LA MORA', 'GUARAMBARÉ', 'ITÁ', 'ITAUGUÁ', 'J. AUGUSTO SALDÍVAR', 'LAMBARÉ', 'LIMPIO', 'LUQUE', 'MARIANO ROQUE ALONSO', 'ÑEMBY', 'NUEVA ITALIA', 'SAN ANTONIO', 'SAN LORENZO', 'VILLA ELISA', 'VILLETTA', 'YPACARAÍ', 'YPANÉ'},
            'ÑEEMBUCÚ': {'PILAR', 'CERRITO', 'DESMOCHADOS', 'GENERAL JOSÉ EDUVIGIS DÍAS', 'GUAZÚ CUÁ', 'HUMAITÁ', 'ISLA UMBÚ', 'LAURELES', 'PASO DE PATRIA', 'SAN JUAN BAUTISTA DE ÑEEMBUCÚ', 'TACUARAS', 'VILLA FRANCA', 'VILLA OLIVA', 'VILLALBÍN'},
            'AMAMBAY': {'PEDRO JUAN CABALLERO', 'BELLA VISTA NORTE', 'CAPITÁN BADO', 'KARAPAÍ', 'ZANJA PYTÁ', 'CERRO CORÁ'},
            'CANINDEYÚ': {'SALTO DEL GUAIRÁ', 'CORPUS CHRISTI', 'CURUGUATY', 'GENERAL FRANCISCO CABALLERO ÁLVAREZ', 'ITANARÁ', 'KATUETÉ', 'LA PALOMA', 'NUEVA ESPERANZA', 'VILLA YGATIMÍ', 'YASY CAÑY', 'YPEJHÚ', 'LAUREL', 'PUERTO ADELA', 'MARACANÁ', 'YBY PYTA', 'YBYRAROBANÁ'},
            'PDTE. HAYES': {'VILLA HAYES', 'BENJAMÍN ACEVAL', 'DOCTOR JOSÉ FALCÓN', 'GENERAL BRUGUEZ', 'NANAWA', 'PUERTO PINASCO', 'TENIENTE IRALA FERNÁNDEZ', 'NUEVA ASUNCIÓN', 'CAMPO ACEVAL', 'TENIENTE ESTEBAN MARTÍNEZ'},
            'ALTO PARAGUAY': {'FUERTE OLIMPO', 'BAHÍA NEGRA', 'CAPITÁN CARMELO PERALTA', 'PUERTO CASADO'},
            'BOQUERON': {'FILADELFIA', 'LOMA PLATA', 'MARISCAL ESTIGARRIBIA', 'BOQUERÓN'}
            }

        # Creamos un conjunto con todos los distritos válidos para búsquedas rápidas
        self.todos_distritos_validos = set()
        for distritos in self.DISTRITOS_POR_DEPARTAMENTO.values():
            self.todos_distritos_validos.update(distritos)

        # Mapeo de distrito a departamento para cuando solo tenemos el distrito
        self.distrito_a_departamento = {}
        for depto, distritos in self.DISTRITOS_POR_DEPARTAMENTO.items():
            for distrito in distritos:
                self.distrito_a_departamento[distrito] = depto

        # Diccionario para corregir nombres de distritos
        self.estandarizacion_distritos = self._construir_diccionario_distritos()

        # Preparamos versiones normalizadas de todos los diccionarios
        # Esto nos ayuda a comparar textos sin importar acentos o mayúsculas
        def _norm_str(s):
            """Limpia un texto para comparación: quita acentos, convierte a mayúsculas y elimina espacios extra."""
            if s is None:
                return ''
            try:
                s2 = str(s).upper().strip()
                s2 = unicodedata.normalize('NFKD', s2)
                s2 = ''.join(ch for ch in s2 if not unicodedata.combining(ch))
                s2 = re.sub(r'\s+', ' ', s2)
                return s2
            except Exception:
                return str(s).upper().strip()

        self._norm_str = _norm_str

        # Cargar datos de barrios y localidades desde JSON
        self.barrios_por_distrito = self._cargar_barrios_desde_json()
        self.todas_localidades_validas = self._preparar_localidades_validas()

        

        # Creamos versiones normalizadas de todos nuestros diccionarios
        self.estandarizacion_dept_norm = {}
        for k, v in self.estandarizacion_dept.items():
            self.estandarizacion_dept_norm[_norm_str(k)] = v

        self.distrito_a_departamento_norm = {}
        for k, v in self.distrito_a_departamento.items():
            self.distrito_a_departamento_norm[_norm_str(k)] = v

        self.estandarizacion_distritos_norm = {}
        for k, v in self.estandarizacion_distritos.items():
            self.estandarizacion_distritos_norm[_norm_str(k)] = v

        self.todos_distritos_validos_norm = {self._norm_str(d) for d in self.todos_distritos_validos}

        # Diccionario para categorizar eventos de manera consistente
        # También marcamos qué registros deben eliminarse (como preposicionamientos)
        self.estandarizacion_eventos = {
            # COVID: Todas las variantes relacionadas con la pandemia
            'ALB.COVID': 'COVID', 'ALBER.COVID': 'COVID', 'ALBERG.COVID': 'COVID',
            'COVI 19 OLL.': 'COVID', 'COVID 19': 'COVID', 'COVI': 'COVID',
            'VAC.ARATIRI': 'COVID', 'VACUNATORIO SND': 'COVID',
            'APOY.INST.COVID 19': 'COVID', 'APOYO INSTITUCIONAL COVID': 'COVID',
            'ÑANGARECO': 'COVID', 'ÑANGAREKO': 'COVID',

            # INCENDIO: Eventos relacionados con fuego
            'INC.FORESTAL': 'INCENDIO', 'INCCENDIO': 'INCENDIO', 'INCEND': 'INCENDIO',
            'INCEND. DOMIC.': 'INCENDIO', 'INCENDIO DOMICILIARIO': 'INCENDIO',
            'DERRUMBE': 'INCENDIO', 'INCENDIO FORESTAL': 'INCENDIO',

            # TORMENTA SEVERA: Eventos climáticos
            'EVENTO CLIMATICO': 'TORMENTA SEVERA', 'TORMENTA SEVERA CENTRAL': 'TORMENTA SEVERA',
            'EVENTO CLIMATICO TEMPORAL': 'TORMENTA SEVERA', 'MUNICIPALIDAD': 'TORMENTA SEVERA',
            'TEMPORAL': 'TORMENTA SEVERA', 'TEMPORAL CENTRAL': 'TORMENTA SEVERA',
            'TEMPORAL - MUNICIPALIDAD': 'TORMENTA SEVERA', 'TEMPORAL-GOBERNACION': 'TORMENTA SEVERA',
            'TEMPORAL - GOBERNACION': 'TORMENTA SEVERA', 'TEMPORAL-GOBERNACIÓN': 'TORMENTA SEVERA',
            'TEMPORAL - GOBERNACIÓN': 'TORMENTA SEVERA', 'TEMPORAL CENTRAL MUNICIPALIDAD': 'TORMENTA SEVERA',

            # SEQUIA: Falta de agua prolongada
            'SEQ. E INUND.': 'SEQUIA', 'SEQ./INUND.': 'SEQUIA', 'SEQUIA-INUND.': 'SEQUIA',

            # EXTREMA VULNERABILIDAD: Ayuda social general
            'COMISION VECINAL': 'EXTREMA VULNERABILIDAD',
            'AYUDA SOLIDARIA': 'EXTREMA VULNERABILIDAD',

            # C.I.D.H.: Casos relacionados con la Corte Interamericana
            'Asistencia de la corte': 'C.I.D.H.',
            'ASISTENCIA DE LA CORTE': 'C.I.D.H.',
            'C I D H': 'C.I.D.H.', 'C.H.D.H': 'C.I.D.H.', 'C.I.D.H': 'C.I.D.H.',
            'C.I.D.H.': 'C.I.D.H.', 'C.ID.H': 'C.I.D.H.', 'CIDH': 'C.I.D.H.',

            # OPERATIVO JAHO'I: Operativos especiales
            'OPERATIVO ÑEÑUA': "OPERATIVO JAHO'I", 'OPERATIVO ESPECIAL': "OPERATIVO JAHO'I",
            'OP INVIERNO': "OPERATIVO JAHO'I", 'OP. INVIERNO': "OPERATIVO JAHO'I",
            'OP. ÑEÑUA': "OPERATIVO JAHO'I", 'OP.INVIERNO': "OPERATIVO JAHO'I",

            # INUNDACION: Eventos de agua
            'INUNDAC.': 'INUNDACION', 'INUNDAIÓN S.': 'INUNDACION',
            'INUNDACION SUBITA': 'INUNDACION', 'INUNDACION " DECLARACION DE EMERGENCIA"': 'INUNDACION',
            'LNUNDACION': 'INUNDACION', 'INUNDACIÓN': 'INUNDACION',

            # OLLA POPULAR: Programas de alimentación
            'OLLA P': 'OLLA POPULAR', 'OLLA P.': 'OLLA POPULAR', 'OLLA POP': 'OLLA POPULAR',
            'OLLA POP.': 'OLLA POPULAR', 'OLLA POPILAR': 'OLLA POPULAR',
            'OLLA POPOLAR': 'OLLA POPULAR', 'OLLA POPUL': 'OLLA POPULAR',
            'OLLAP.': 'OLLA POPULAR', 'OLLA POPULAR COVID': 'OLLA POPULAR',

            # OTROS: Categoría para eventos misceláneos
            'INERAM': 'OTROS', 'INERAM(MINGA)': 'OTROS', 'MINGA': 'OTROS',
            'INDERT': 'OTROS', 'INDI MBYA GUARANI': 'OTROS', 'NIÑEZ': 'OTROS',
            'DGRR 027/22': 'OTROS', 'DGRR 028/22': 'OTROS', 'DONAC': 'OTROS',
            'DONAC.': 'OTROS', 'DONACIÒN': 'OTROS', 'EDAN': 'OTROS',
            'EVALUACION DE DAÑOS': 'OTROS', 'TRABAJO COMUNITARIO': 'OTROS',
            'ASISTENCIA INSTITUCIONAL': 'OTROS', 'APOYO LOGISTICO': 'OTROS',
            'APOYO INSTITUCIONAL': 'OTROS', 'APOY.LOG': 'OTROS', 'APOY LOG': 'OTROS',
            'APOYO LOG.': 'OTROS', 'OTROS "TEMPORAL"': 'OTROS',
            'APOYO LOGISTICO INDI': 'OTROS',

            # PREPOSICIONAMIENTO: Registros que deben eliminarse (no son asistencia real)
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

            # SIN EVENTO: Cuando no se especifica causa
            'SIN_EVENTO': 'SIN EVENTO', 'DEVOLVIO': 'SIN EVENTO',
            'REFUGIO SEN': 'SIN EVENTO', '': 'SIN EVENTO',
            'SIN EVENTO': 'SIN EVENTO'
        }

        # Cache para guardar resultados de comparaciones y hacer el proceso más rápido
        self._similitud_cache = {}

    def _cargar_barrios_desde_json(self, ruta_json="barrios_por_distrito.json"):
        """Carga el archivo JSON con los barrios organizados por distrito.
        
        Este archivo nos ayuda a validar y corregir localidades usando
        información oficial sobre qué barrios existen en cada distrito.
        """
        try:
            # Buscar el archivo en varias ubicaciones posibles
            posibles_rutas = [
                Path(ruta_json),
                Path(__file__).parent / ruta_json,
                Path(__file__).parent.parent / ruta_json
            ]
            
            archivo_encontrado = None
            for ruta in posibles_rutas:
                if ruta.exists():
                    archivo_encontrado = ruta
                    break
            
            if not archivo_encontrado:
                print("⚠️  Archivo de barrios no encontrado. Continuando sin validación de localidades.")
                return {}
            
            with open(archivo_encontrado, 'r', encoding='utf-8') as f:
                barrios_data = json.load(f)
            
            # Normalizar los nombres para comparaciones consistentes
            barrios_normalizados = {}
            for distrito, barrios in barrios_data.items():
                distrito_norm = self._norm_str(distrito)
                barrios_norm = [self._norm_str(barrio) for barrio in barrios]
                barrios_normalizados[distrito_norm] = barrios_norm
            
            print(f"✅ Cargados barrios para {len(barrios_normalizados)} distritos desde {archivo_encontrado}")
            return barrios_normalizados
            
        except Exception as e:
            print(f"⚠️  Error cargando archivo de barrios: {e}")
            return {}

    def _preparar_localidades_validas(self):
        """Prepara un conjunto con todas las localidades válidas para búsquedas rápidas."""
        todas_localidades = set()
        if self.barrios_por_distrito:
            for barrios in self.barrios_por_distrito.values():
                todas_localidades.update(barrios)
            print(f"✅ Preparadas {len(todas_localidades)} localidades válidas para validación")
        return todas_localidades

    def _construir_diccionario_distritos(self):
        """Construye un diccionario completo para corregir nombres de distritos."""
        estandarizacion = {
            # CIUDAD DEL ESTE y variantes
            'C.D.E': 'CIUDAD DEL ESTE',
            'CDE': 'CIUDAD DEL ESTE', 
            'C DE E': 'CIUDAD DEL ESTE',
            'CIUDAD DEL ESTE': 'CIUDAD DEL ESTE',
            'C. DEL ESTE': 'CIUDAD DEL ESTE',
            
            # ASUNCIÓN y variantes
            'ASUNCION': 'ASUNCIÓN',
            'ASUNCIÒN': 'ASUNCIÓN',
            'ASU': 'ASUNCIÓN',
            'A.S.N.C': 'ASUNCIÓN',
            
            # ENCARNACIÓN y variantes
            'ENCARNACION': 'ENCARNACIÓN',
            'ENCARNACIÒN': 'ENCARNACIÓN',
            'ENC': 'ENCARNACIÓN',
            
            # CORONEL OVIEDO y variantes
            'CNEL OVIEDO': 'CORONEL OVIEDO',
            'CNEL. OVIEDO': 'CORONEL OVIEDO',
            'C. OVIEDO': 'CORONEL OVIEDO',
            'CORONEL OVIEDO': 'CORONEL OVIEDO',
            
            # PEDRO JUAN CABALLERO y variantes
            'PEDRO JUAN C.': 'PEDRO JUAN CABALLERO',
            'PEDRO JUAN': 'PEDRO JUAN CABALLERO',
            'PJC': 'PEDRO JUAN CABALLERO',
            'PEDRO JUAN CABALLERO': 'PEDRO JUAN CABALLERO',
            
            # VILLARRICA y variantes
            'VILLA RICA': 'VILLARRICA',
            'VILLARICA': 'VILLARRICA',
            
            # CONCEPCIÓN y variantes
            'CONCEPCION': 'CONCEPCIÓN',
            'CONCEPCIÒN': 'CONCEPCIÓN',
            
            # CAACUPÉ y variantes
            'CAACUPE': 'CAACUPÉ',
            'CAACUPÈ': 'CAACUPÉ',
            
            # CAPIATÁ y variantes
            'CAPIATA': 'CAPIATÁ',
            'CAPIATÀ': 'CAPIATÁ',
            
            # ITÁ y variantes
            'ITÀ': 'ITÁ',
            'ITA': 'ITÁ',
            
            # LAMBARÉ y variantes
            'LAMBARE': 'LAMBARÉ',
            'LAMBARÈ': 'LAMBARÉ',
            
            # YPANÉ y variantes
            'YPANE': 'YPANÉ',
            'YPANÈ': 'YPANÉ',
            
            # PRESIDENTE FRANCO y variantes
            'PTE FRANCO': 'PRESIDENTE FRANCO',
            'P.FRANCO': 'PRESIDENTE FRANCO',
            'PRES. FRANCO': 'PRESIDENTE FRANCO',
            
            # HERNANDARIAS y variantes
            'HERNANDARIAS': 'HERNANDARIAS',
            'HERNANDARÍAS': 'HERNANDARIAS',
            
            # SAN LORENZO y variantes
            'SAN LORENZO': 'SAN LORENZO',
            'S.LORENZO': 'SAN LORENZO',
            
            # FERNANDO DE LA MORA y variantes
            'FDO DE LA MORA': 'FERNANDO DE LA MORA',
            'FERNANDO MORA': 'FERNANDO DE LA MORA',
            'F. DE LA MORA': 'FERNANDO DE LA MORA',
            
            # LAMBARÉ y variantes
            'LAMBARE': 'LAMBARÉ',
            
            # ÑEMBY y variantes
            'NEMBY': 'ÑEMBY',
            
            # MARIANO ROQUE ALONSO y variantes
            'MROQUE ALONSO': 'MARIANO ROQUE ALONSO',
            'M. ROQUE ALONSO': 'MARIANO ROQUE ALONSO',
            'MROA': 'MARIANO ROQUE ALONSO',
            
            # VILLA ELISA y variantes
            'VILLA ELISA': 'VILLA ELISA',
            'V. ELISA': 'VILLA ELISA',
            
            # LIMPIO y variantes
            'LIMPIO': 'LIMPIO',
            'LIMPIO CENTRO': 'LIMPIO',
            
            # LUQUE y variantes
            'LUQUE': 'LUQUE',
            'LUQUE CENTRO': 'LUQUE',
            
            # AREGUÁ y variantes
            'AREGUA': 'AREGUÁ',
            'AREGUÀ': 'AREGUÁ',
            
            # GUARAMBARÉ y variantes
            'GUARAMBARE': 'GUARAMBARÉ',
            'GUARAMBARÈ': 'GUARAMBARÉ',
            
            # ITAUGUÁ y variantes
            'ITAUGA': 'ITAUGUÁ',
            'ITAUGAU': 'ITAUGUÁ',
            
            # YPACARAÍ y variantes
            'YPACARAI': 'YPACARAÍ',
            'YPACARAÌ': 'YPACARAÍ',
            
            # VILLETTA y variantes
            'VILLETA': 'VILLETTA',
            'VILLET A': 'VILLETTA',
            
            # SALTO DEL GUAIRÁ y variantes
            'SALTO GUAIRA': 'SALTO DEL GUAIRÁ',
            'SALTO DEL GUAIRA': 'SALTO DEL GUAIRÁ',
            
            # FILADELFIA y variantes
            'FILADELFIA': 'FILADELFIA',
            'FILADELF.': 'FILADELFIA',
            
            # LOMA PLATA y variantes
            'LOMA PLATA': 'LOMA PLATA',
            'L. PLATA': 'LOMA PLATA',
            
            # MARISCAL ESTIGARRIBIA y variantes
            'M. ESTIGARRIBIA': 'MARISCAL ESTIGARRIBIA',
            'MARISCAL ESTIG.': 'MARISCAL ESTIGARRIBIA',
            
            # BELLA VISTA NORTE y variantes
            'BELLA VISTA N.': 'BELLA VISTA NORTE',
            'B.VISTA NORTE': 'BELLA VISTA NORTE',
            
            # CAPITÁN BADO y variantes
            'CAPITAN BADO': 'CAPITÁN BADO',
            'CAP. BADO': 'CAPITÁN BADO',
            
            # FUERTE OLIMPO y variantes
            'F. OLIMPO': 'FUERTE OLIMPO',
            'FUERTE OL.': 'FUERTE OLIMPO',
            
            # PUERTO CASADO y variantes
            'PTO CASADO': 'PUERTO CASADO',
            'P. CASADO': 'PUERTO CASADO'
        }
        
        return estandarizacion

    def _calcular_similitud(self, texto1, texto2):
        """Calcula qué tan parecidos son dos textos.
        
        Usa un algoritmo que cuenta cuántos cambios se necesitan para
        convertir un texto en otro. Entre más bajo el número, más parecidos son.
        """
        key = (texto1, texto2)
        if key in self._similitud_cache:
            return self._similitud_cache[key]
        
        # Normalizamos los textos para comparar sin importar acentos o mayúsculas
        norm1 = self._norm_str(texto1)
        norm2 = self._norm_str(texto2)
        
        # Calculamos la similitud (0 , 1)
        similitud = Levenshtein.ratio(norm1, norm2)
        
        self._similitud_cache[key] = similitud
        return similitud

    def _buscar_mejor_coincidencia(self, texto, opciones, umbral=0.7):
        """Encuentra la opción que más se parece al texto dado.
        
        Primero busca coincidencia exacta, y si no encuentra, busca
        la opción más similar que supere el umbral de parecido.
        """
        if not texto or not opciones:
            return None
        
        texto_norm = self._norm_str(texto)
        
        # Primero intentamos encontrar una coincidencia exacta
        for opcion in opciones:
            if self._norm_str(opcion) == texto_norm:
                return opcion
        
        # Si no hay exacta, buscamos la más similar
        mejor_coincidencia = None
        mejor_puntaje = 0
        
        for opcion in opciones:
            puntaje = self._calcular_similitud(texto, opcion)
            if puntaje > mejor_puntaje and puntaje >= umbral:
                mejor_puntaje = puntaje
                mejor_coincidencia = opcion
        
        return mejor_coincidencia

    def _es_localidad_valida_en_json(self, distrito, localidad):
        """Verifica si una localidad existe exactamente en el JSON para ese distrito."""
        if not self.barrios_por_distrito:
            return False
            
        distrito_norm = self._norm_str(distrito)
        localidad_norm = self._norm_str(localidad)
        
        return (distrito_norm in self.barrios_por_distrito and 
                localidad_norm in self.barrios_por_distrito[distrito_norm])

    def _buscar_localidad_en_json(self, distrito, localidad, umbral=0.85):
        """Busca una localidad en el JSON del distrito usando Levenshtein."""
        if not self.barrios_por_distrito:
            return None
            
        distrito_norm = self._norm_str(distrito)
        if distrito_norm not in self.barrios_por_distrito:
            return None
        
        barrios_del_distrito = self.barrios_por_distrito[distrito_norm]
        return self._buscar_mejor_coincidencia(localidad, barrios_del_distrito, umbral=umbral)

    def _buscar_localidad_en_todos_distritos(self, localidad, umbral=0.8):
        """Busca una localidad en todos los distritos del JSON."""
        if not self.barrios_por_distrito:
            return None, None
        
        # Recolectar todos los barrios de todos los distritos
        todos_los_barrios = []
        mapeo_barrio_a_distrito = {}
        
        for distrito, barrios in self.barrios_por_distrito.items():
            for barrio in barrios:
                todos_los_barrios.append(barrio)
                mapeo_barrio_a_distrito[barrio] = distrito
        
        mejor_coincidencia = self._buscar_mejor_coincidencia(localidad, todos_los_barrios, umbral=umbral)
        
        if mejor_coincidencia:
            distrito_correcto = mapeo_barrio_a_distrito.get(mejor_coincidencia)
            return mejor_coincidencia, distrito_correcto
        
        return None, None

    def limpiar_texto(self, texto):
        """Limpia un texto: quita espacios extra, convierte a mayúsculas y maneja valores vacíos.
        
        Esta es una de las transformaciones más básicas pero importantes.
        Asegura que todos los textos sigan el mismo formato.
        """
        if pd.isna(texto) or texto is None or str(texto).strip() == '':
            return 'SIN ESPECIFICAR'
        return str(texto).strip().upper()

    def limpiar_numero(self, value):
        """Convierte un valor a número entero de manera segura.
        
        Acepta números con coma o punto decimal y los convierte a enteros.
        Si no puede convertirlo, devuelve 0 en lugar de generar error.
        """
        try:
            # Aceptamos formatos como '1,5' o '1.5'
            if isinstance(value, str):
                value = value.replace(',', '.')
            return int(float(value)) if value not in [None, '', np.nan] else 0
        except (ValueError, TypeError):
            return 0

    def estandarizar_departamento_robusto(self, departamento):
        """Convierte cualquier variante de nombre de departamento al nombre oficial.
        
        Sigue una estrategia de 4 pasos:
        1. Busca en el diccionario de correcciones
        2. Si tiene separadores, prueba con la primera parte
        3. Busca coincidencias parciales
        4. Si no encuentra, usa 'CENTRAL' por defecto
        """
        if pd.isna(departamento) or departamento is None:
            return 'CENTRAL'

        depto_limpio = self.limpiar_texto(departamento)
        depto_norm = self._norm_str(depto_limpio)

        # Si el usuario puso un distrito en lugar del departamento, lo corregimos
        if depto_norm in self.distrito_a_departamento_norm:
            return self.distrito_a_departamento_norm[depto_norm]

        # 1. Busqueda directa en el diccionario de correcciones
        if depto_norm in self.estandarizacion_dept_norm:
            return self.estandarizacion_dept_norm[depto_norm]

        # 2. Si el texto contiene separadores, probamos con la primera parte
        for sep in [' - ', ' / ', ', ', ' Y ']:
            if sep in depto_limpio:
                primera_parte = depto_limpio.split(sep)[0].strip()
                if primera_parte in self.estandarizacion_dept:
                    return self.estandarizacion_dept[primera_parte]

        # 3. Buscamos nombres oficiales dentro del texto
        for depto_estandar in self.departamento_orden.keys():
            if self._norm_str(depto_estandar) in depto_norm:
                return depto_estandar

        # 4. Si no logramos identificar, usamos Central por defecto
        return 'CENTRAL'

    def estandarizar_distrito_robusto(self, distrito, departamento=None):
        """Normaliza nombres de distritos usando información contextual y similitud.
        
        Usa el departamento como pista para buscar solo en distritos de esa zona,
        haciendo la búsqueda más precisa.
        """
        if pd.isna(distrito) or distrito is None:
            return 'SIN ESPECIFICAR'

        distrito_limpio = self.limpiar_texto(distrito)
        
        # Si ya está marcado como sin especificar, no hacemos nada
        if distrito_limpio == 'SIN ESPECIFICAR':
            return distrito_limpio

        distrito_norm = self._norm_str(distrito_limpio)

        # 1. Busqueda directa en correcciones conocidas
        if distrito_norm in self.estandarizacion_distritos_norm:
            return self.estandarizacion_distritos_norm[distrito_norm]

        # 2. Si sabemos el departamento, buscamos solo en sus distritos
        opciones_busqueda = self.todos_distritos_validos
        if departamento:
            depto_estandar = self.estandarizar_departamento_robusto(departamento)
            if depto_estandar in self.DISTRITOS_POR_DEPARTAMENTO:
                opciones_busqueda = self.DISTRITOS_POR_DEPARTAMENTO[depto_estandar]

        # 3. Buscamos la opción más similar
        mejor_coincidencia = self._buscar_mejor_coincidencia(
            distrito_limpio, opciones_busqueda, umbral=0.6
        )

        if mejor_coincidencia:
            return mejor_coincidencia

        # 4. Si no encontramos en el departamento, buscamos en todos los distritos
        if departamento:
            mejor_coincidencia = self._buscar_mejor_coincidencia(
                distrito_limpio, self.todos_distritos_validos, umbral=0.6
            )
            if mejor_coincidencia:
                return mejor_coincidencia

        # 5. Si todo falla, mantenemos el texto original pero limpio
        return distrito_limpio

    def corregir_distrito_en_localidad(self, localidad, distrito_actual):
        """Detecta cuando alguien puso el distrito en el campo de localidad.
        
        Esto pasa frecuentemente cuando la persona llena los formularios.
        Si detectamos un distrito en localidad y el campo distrito está vacío,
        movemos el valor al campo correcto.
        """
        if pd.isna(localidad) or localidad is None:
            return localidad, distrito_actual

        localidad_limpia = self.limpiar_texto(localidad)
        
        if localidad_limpia == 'SIN ESPECIFICAR':
            return localidad_limpia, distrito_actual

        # Buscamos si la localidad coincide con algún distrito válido
        distrito_en_localidad = self._buscar_mejor_coincidencia(
            localidad_limpia, self.todos_distritos_validos, umbral=0.8
        )

        if distrito_en_localidad:
            # Encontramos un distrito en la localidad
            if distrito_actual in ['SIN ESPECIFICAR', '']:
                # Si el distrito está vacío, movemos el valor
                return 'SIN ESPECIFICAR', distrito_en_localidad
            else:
                # Si ya hay distrito, solo limpiamos la localidad
                return 'SIN ESPECIFICAR', distrito_actual

        return localidad_limpia, distrito_actual

    def estandarizar_localidad_robusta(self, localidad, distrito_estandarizado):
        """Limpia y valida nombres de localidades usando JSON y Levenshtein.
        
        Sigue una estrategia combinada:
        1. Verificación exacta en JSON (rápido y preciso)
        2. Búsqueda por similitud en JSON del distrito
        3. Búsqueda global en todos los barrios
        4. Reglas heurísticas como fallback
        """
        if pd.isna(localidad) or localidad is None:
            return 'SIN ESPECIFICAR'

        localidad_limpia = self.limpiar_texto(localidad)
        
        if localidad_limpia == 'SIN ESPECIFICAR':
            return localidad_limpia

        # Si la localidad es igual al distrito, no tiene sentido repetir
        if localidad_limpia == distrito_estandarizado:
            return 'SIN ESPECIFICAR'

        # ESTRATEGIA 1: Verificación exacta en JSON
        if self._es_localidad_valida_en_json(distrito_estandarizado, localidad_limpia):
            return localidad_limpia  # ¡Encontrado exactamente!

        # ESTRATEGIA 2: Búsqueda por similitud en el distrito actual
        correccion_local = self._buscar_localidad_en_json(distrito_estandarizado, localidad_limpia, umbral=0.85)
        if correccion_local:
            return correccion_local

        # ESTRATEGIA 3: Búsqueda global en todos los distritos
        if localidad_limpia not in ['', 'SIN ESPECIFICAR'] and len(localidad_limpia) > 3:
            correccion_global, distrito_correcto = self._buscar_localidad_en_todos_distritos(localidad_limpia, umbral=0.8)
            if correccion_global:
                return correccion_global

        # ESTRATEGIA 4: Reglas heurísticas (fallback)
        palabras_clave_localidad = ['BARRIO', 'COLONIA', 'COMUNIDAD', 'ALDEA', 'ASENTAMIENTO', 'CANTON']
        for palabra in palabras_clave_localidad:
            if palabra in localidad_limpia:
                return localidad_limpia

        # Nombres muy cortos probablemente son errores
        if len(localidad_limpia) < 3:
            return 'SIN ESPECIFICAR'

        # Si pasó todas las validaciones, la mantenemos
        return localidad_limpia

    def estandarizar_evento_robusto(self, evento):
        """Categoriza eventos en tipos estandarizados.
        
        Convierte las muchas formas en que la gente describe los eventos
        en categorías consistentes para análisis.
        """
        if pd.isna(evento) or evento is None:
            return 'SIN EVENTO'

        evento_limpio = self.limpiar_texto(evento)

        # 1. Busqueda directa en el diccionario
        if evento_limpio in self.estandarizacion_eventos:
            return self.estandarizacion_eventos[evento_limpio]

        # 2. Busqueda por palabras clave dentro del texto
        palabras_clave = {
            'COVID': 'COVID', 'INCENDIO': 'INCENDIO', 'TORMENTA': 'TORMENTA SEVERA',
            'TEMPORAL': 'TORMENTA SEVERA', 'INUNDACION': 'INUNDACION',
            'SEQUIA': 'SEQUIA', 'JAHO': "OPERATIVO JAHO'I", 'ÑEÑUA': "OPERATIVO JAHO'I",
            'OLLA': 'OLLA POPULAR', 'VULNERABILIDAD': 'EXTREMA VULNERABILIDAD',
            'CIDH': 'C.I.D.H.',
            'Corte': 'C.I.D.H.',
            'CORTE': 'C.I.D.H.',
        }

        for palabra, evento_estandar in palabras_clave.items():
            if palabra in evento_limpio:
                return evento_estandar

        # 3. Si no coincide con nada, marcamos como sin evento
        return 'SIN EVENTO'

    def normalize_locations(self, df):
        """Orquesta la normalización completa de todas las columnas de ubicación.
        
        Sigue un orden específico porque cada paso depende del anterior:
        1. Departamentos (la base)
        2. Distritos (usando departamento como contexto)
        3. Corrección de distritos en localidades
        4. Localidades (limpieza final con JSON y Levenshtein)
        """
        if df is None or len(df) == 0:
            return df

        # Aseguramos que existan las columnas mínimas
        for col in ['LOCALIDAD', 'DISTRITO', 'DEPARTAMENTO']:
            if col not in df.columns:
                df[col] = 'SIN ESPECIFICAR'

        print("  🗺️  Estandarizando ubicaciones...")
        cambios = 0

        # 1. Primero estandarizamos departamentos (la base geográfica)
        if 'DEPARTAMENTO' in df.columns:
            for idx, row in df.iterrows():
                depto_original = row.get('DEPARTAMENTO', '')
                depto_estandarizado = self.estandarizar_departamento_robusto(depto_original)
                if depto_estandarizado != depto_original:
                    df.at[idx, 'DEPARTAMENTO'] = depto_estandarizado
                    cambios += 1

        # 2. Luego estandarizamos distritos (con información del departamento)
        if 'DISTRITO' in df.columns:
            for idx, row in df.iterrows():
                distrito_original = row.get('DISTRITO', '')
                departamento_actual = row.get('DEPARTAMENTO', '')
                distrito_estandarizado = self.estandarizar_distrito_robusto(
                    distrito_original, departamento_actual
                )
                if distrito_estandarizado != distrito_original:
                    df.at[idx, 'DISTRITO'] = distrito_estandarizado
                    cambios += 1
        
        # 2b. Corregimos departamentos basados en distritos estandarizados
        for idx, row in df.iterrows():
            distrito_actual = row.get('DISTRITO', '')
            departamento_actual = row.get('DEPARTAMENTO', '')
            
            # Si el distrito es conocido, obtenemos su departamento correcto
            distrito_norm = self._norm_str(distrito_actual)
            if distrito_norm in self.distrito_a_departamento_norm:
                departamento_correcto = self.distrito_a_departamento_norm[distrito_norm]
                
                # Si el departamento actual no coincide, lo corregimos
                if departamento_actual != departamento_correcto:
                    df.at[idx, 'DEPARTAMENTO'] = departamento_correcto
                    cambios += 1

        # 3. Detectamos y corregimos distritos en el campo de localidad
        for idx, row in df.iterrows():
            localidad_original = row.get('LOCALIDAD', '')
            distrito_actual = row.get('DISTRITO', '')
            
            localidad_corregida, distrito_corregido = self.corregir_distrito_en_localidad(
                localidad_original, distrito_actual
            )
            
            if localidad_corregida != localidad_original:
                df.at[idx, 'LOCALIDAD'] = localidad_corregida
                cambios += 1
            
            if distrito_corregido != distrito_actual:
                df.at[idx, 'DISTRITO'] = distrito_corregido
                cambios += 1

        # 4. Finalmente estandarizamos localidades (¡ahora con JSON y Levenshtein!)
        for idx, row in df.iterrows():
            localidad_actual = row.get('LOCALIDAD', '')
            distrito_estandarizado = row.get('DISTRITO', '')
            
            localidad_estandarizada = self.estandarizar_localidad_robusta(
                localidad_actual, distrito_estandarizado
            )
            
            if localidad_estandarizada != localidad_actual:
                df.at[idx, 'LOCALIDAD'] = localidad_estandarizada
                cambios += 1

        # Reportamos cuántos cambios hicimos
        if cambios > 0:
            print(f"  ✅ Normalización de ubicaciones aplicada. Cambios realizados: {cambios}")
        else:
            print("  ℹ️  Normalización de ubicaciones: no se detectaron cambios relevantes.")

        return df

    def post_process_eventos_with_aids(self, row):
        """Infiere el tipo de evento cuando no está especificado.
        
        Usa pistas como los tipos de insumos entregados y la ubicación
        para adivinar qué tipo de evento causó la ayuda.
        """
        evento = row.get('EVENTO', 'SIN EVENTO')

        # Filtramos registros de preposicionamiento (no son ayuda real)
        if evento == 'ELIMINAR_REGISTRO':
            return 'ELIMINAR_REGISTRO'

        # Solo inferimos si no hay evento especificado
        if evento == 'SIN EVENTO' or evento == '' or evento is None:
            departamento = str(row.get('DEPARTAMENTO', '')).upper()

            # REGLA 1: Departamentos tradicionalmente secos → SEQUIA
            if departamento in ['BOQUERON', 'ALTO PARAGUAY', 'PDTE. HAYES']:
                return 'SEQUIA'

            # Calculamos cantidades de insumos
            kit_b = self.limpiar_numero(row.get('KIT B', row.get('KIT_B', 0)))
            kit_a = self.limpiar_numero(row.get('KIT A', row.get('KIT_A', 0)))
            total_kits = kit_b + kit_a

            chapa_zinc = self.limpiar_numero(row.get('CHAPA ZINC', row.get('CHAPA_ZINC', 0)))
            chapa_fibrocemento = self.limpiar_numero(row.get('CHAPA FIBROCEMENTO', row.get('CHAPA_FIBROCEMENTO', 0)))

            # Sumamos materiales no kits
            materiales_no_kits_cols = [
                'CHAPA FIBROCEMENTO', 'CHAPA_FIBROCEMENTO', 'CHAPA ZINC', 'CHAPA_ZINC',
                'COLCHONES', 'FRAZADAS', 'TERCIADAS', 'PUNTALES', 'CARPAS PLASTICAS', 'CARPAS_PLASTICAS'
            ]
            materiales = sum(self.limpiar_numero(row.get(field, 0)) for field in materiales_no_kits_cols)
            total_insumos = total_kits + materiales

            # REGLA 2: Pocos kits + materiales → INCENDIO
            if total_kits < 10 and total_kits > 0 and materiales > 0:
                return 'INCENDIO'

            # REGLA 4: En capital, solo kits → INUNDACION
            if departamento == 'CAPITAL' and total_kits > 0 and materiales == 0:
                return 'INUNDACION'

            # REGLA 5: Solo chapa zinc → TORMENTA SEVERA
            if chapa_zinc > 0 and total_kits == 0 and chapa_fibrocemento == 0:
                return 'TORMENTA SEVERA'

            # REGLA 6: Solo chapa fibrocemento → INUNDACION
            if chapa_fibrocemento > 0 and total_kits == 0 and chapa_zinc == 0:
                return 'INUNDACION'

            # REGLA 7: Si hay kits → EXTREMA VULNERABILIDAD
            if total_kits > 0:
                return 'EXTREMA VULNERABILIDAD'
            
            # Si no tiene insumos, marcamos como sin insumos
            if total_insumos == 0:
                return 'SIN_INSUMOS'

            # Por defecto, vulnerabilidad extrema
            return 'EXTREMA VULNERABILIDAD'

        return evento

    def run_complete_correction_pipeline(self, df):
        """Ejecuta todo el proceso de limpieza y transformación.
        
        Esta es la función principal que prepara los datos para el Data Warehouse.
        Sigue un flujo específico para garantizar que los datos estén listos
        para ser cargados en las tablas dimensionales.
        """
        print("🎯 Aplicando estandarización robusta de DEPARTAMENTO, DISTRITO, LOCALIDAD y EVENTO...")

        # Medimos cuántos registros tenemos al inicio
        registros_iniciales = len(df)
        print(f"  Registros iniciales: {registros_iniciales}")

        # Mostrar un estado inicial con ejemplos para facilitar diagnóstico
        print("\n📊 ESTADO INICIAL:")
        # Buscador de columnas por palabra clave (case-insensitive)
        dept_col = next((c for c in df.columns if 'DEPART' in c.upper()), None)
        evento_col = next((c for c in df.columns if 'EVENT' in c.upper()), None)
        distr_col = next((c for c in df.columns if 'DISTRIT' in c.upper()), None)
        loc_col = next((c for c in df.columns if 'LOCALID' in c.upper()), None)

        try:

            # Mostrar distribución inicial por AÑO (si hay columna de fecha)
            date_col = next((c for c in df.columns if 'FECHA' in c.upper()), None)
            if date_col is not None:
                try:
                    parsed_dates = pd.to_datetime(df[date_col], errors='coerce', dayfirst=True, infer_datetime_format=True)
                    year_series = parsed_dates.dt.year.dropna().astype(int)
                    year_counts = year_series.value_counts().sort_index()
                    total_years = int(year_counts.sum()) if len(year_counts) > 0 else 0
                    if total_years > 0:
                        year_counts = year_counts[year_counts.index.map(lambda x: 2018 <= int(x) <= 2023)]
                        if year_counts.sum() == 0:
                            print("\nDistribución por año: No se encontraron años válidos en el rango 2018-2023")
                        else:
                            print("\nDistribución por año:")
                            for y, cnt in year_counts.items():
                                # Porcentaje relativo al total de registros iniciales
                                pct = cnt / registros_iniciales * 100 if registros_iniciales > 0 else 0
                                print(f"- {int(y)}: {int(cnt):,} registros ({pct:.1f}%)")
                    else:
                        print("Distribución por año: No se encontraron años válidos")
                except Exception:
                    print("⚠️ No se pudo parsear la columna de FECHA para obtener años.")

            # Evaluación de completitud por columna
            try:
                print("\nSe evaluó el porcentaje de valores faltantes (nulos o vacíos) por columna:")
                cols_of_interest = ['FECHA', 'DEPARTAMENTO', 'DISTRITO', 'LOCALIDAD', 'EVENTO']
                mapped_cols = []
                for key in cols_of_interest:
                    col = next((c for c in df.columns if key in c.upper()), None)
                    if col:
                        mapped_cols.append((key, col))

                print(f"{'Campo':<12}{'Valores Completos':>18}{'Valores Faltantes':>20}{'% Completitud':>16}")
                for label, colname in mapped_cols:
                    serie = df[colname]
                    completos_mask = serie.notna() & (serie.astype(str).str.strip() != '')
                    completos = int(completos_mask.sum())
                    faltantes = int(registros_iniciales - completos)
                    pct_comp = completos / registros_iniciales * 100 if registros_iniciales > 0 else 0
                    print(f"{label:<12}{completos:18,}{faltantes:20,}{pct_comp:16.1f}%")
            except Exception:
                print("⚠️ Error calculando completitud por columna (diagnóstico inicial).")

            # Ahora imprimimos los recuentos y ejemplos por entidad (Departamentos, Eventos, Distritos, Localidades)
            if dept_col is not None:
                try:
                    dept_counts = df[dept_col].fillna('SIN ESPECIFICAR').astype(str).value_counts(dropna=False)
                    print(f"\n  Departamentos iniciales: {len(dept_counts)}")
                    print(f"  Ejemplos: {dept_counts.head(5).to_dict()}")
                except Exception:
                    pass

            if distr_col is not None:
                try:
                    distr_counts = df[distr_col].fillna('SIN ESPECIFICAR').astype(str).value_counts(dropna=False)
                    print(f"  Distritos iniciales: {len(distr_counts)}")
                    print(f"  Ejemplos: {distr_counts.head(5).to_dict()}")
                except Exception:
                    pass

            if loc_col is not None:
                try:
                    loc_counts = df[loc_col].fillna('SIN ESPECIFICAR').astype(str).value_counts(dropna=False)
                    print(f"  Localidades iniciales: {len(loc_counts)}")
                    print(f"  Ejemplos: {loc_counts.head(5).to_dict()}")
                except Exception:
                    pass

            if evento_col is not None:
                try:
                    evento_counts = df[evento_col].fillna('SIN ESPECIFICAR').astype(str).value_counts(dropna=False)
                    print(f"  Eventos iniciales: {len(evento_counts)}")
                    print(f"  Ejemplos: {evento_counts.head(5).to_dict()}")
                except Exception:
                    pass

        except Exception:
            # No interrumpimos el pipeline por errores al imprimir diagnósticos
            pass

        # --- Análisis exploratorio inicial de insumos y calidad de datos ---
        try:
            # Detectar columnas de insumos por palabras clave
            insumo_keywords = ['KIT', 'CHAPA', 'COLCHON', 'COLCHONES', 'FRAZ', 'TERCIAD', 'PUNTA', 'CARPA']
            insumo_cols = [c for c in df.columns if any(k in c.upper() for k in insumo_keywords)]

            # Construir serie de total de insumos por fila (manejo flexible de tipos)
            if insumo_cols:
                df_insumos_numeric = df[insumo_cols].apply(lambda s: pd.to_numeric(s, errors='coerce').fillna(0))
                total_insumos = df_insumos_numeric.sum(axis=1)

                # Estadísticas descriptivas
                media = total_insumos.mean()
                mediana = total_insumos.median()
                std = total_insumos.std()
                minimo = int(total_insumos.min())
                maximo = int(total_insumos.max())

                print("\nEstadísticas descriptivas de insumos totales por registro:")
                print(f"- Media: {media:,.1f} unidades por asistencia")
                print(f"- Mediana: {int(mediana):,} unidades por asistencia")
                print(f"- Desviación estándar: {std:,.1f}")
                print(f"- Mínimo: {minimo:,} unidades")
                print(f"- Máximo: {maximo:,} unidades")

                # Top 5 registros por total_insumos (usar columnas detectadas para contexto)
                print("\nAsistencias con cantidades excepcionalmente altas:")
                print("Top 5 registros por cantidad total de insumos:")
                top5 = total_insumos.sort_values(ascending=False).head(5)
                for i, idx in enumerate(top5.index, start=1):
                    val = int(total_insumos.at[idx])
                    evento_val = df.at[idx, evento_col] if evento_col in df.columns else 'SIN_EVENTO'
                    dept_val = df.at[idx, dept_col] if dept_col in df.columns else 'SIN_DEPARTAMENTO'
                    fecha_raw = df.at[idx, date_col] if date_col in df.columns else None
                    try:
                        fecha_fmt = pd.to_datetime(fecha_raw, errors='coerce', dayfirst=True)
                        fecha_str = fecha_fmt.strftime('%m/%Y') if not pd.isna(fecha_fmt) else str(fecha_raw)
                    except Exception:
                        fecha_str = str(fecha_raw)
                    print(f"{i}. {val:,} unidades (Evento: {evento_val}, Depto: {dept_val}, Fecha: {fecha_str})")

                # Agregado por año (usar parsed dates si están disponibles)
                try:
                    if date_col in df.columns:
                        parsed_dates = pd.to_datetime(df[date_col], errors='coerce', dayfirst=True)
                        df_year = pd.DataFrame({'year': parsed_dates.dt.year, 'total_insumos': total_insumos})
                        df_year = df_year[df_year['year'].notna()]
                        if not df_year.empty:
                            print("\nExploración Temporal Inicial")
                            print("Asistencias por año:")
                            for y in sorted(df_year['year'].unique()):
                                if 2018 <= int(y) <= 2023:
                                    subset = df_year[df_year['year'] == y]
                                    count_year = int(len(subset))
                                    sum_year = int(subset['total_insumos'].sum())
                                    print(f"{int(y)}: {count_year:,} asistencias | {sum_year:,} unidades totales distribuidas")
                except Exception:
                    pass

                # Métricas de completitud
                try:
                    mandatory_cols = [date_col, dept_col, distr_col, evento_col]
                    mandatory_cols = [c for c in mandatory_cols if c is not None]
                    mask_all = pd.Series(True, index=df.index)
                    for c in mandatory_cols:
                        mask_all &= df[c].notna() & (df[c].astype(str).str.strip() != '')
                    pct_all_fields = mask_all.sum() / registros_iniciales * 100 if registros_iniciales > 0 else 0

                    mask_date_loc = df[date_col].notna() & ((df[dept_col].notna() & (df[dept_col].astype(str).str.strip() != '')) | (df[distr_col].notna() & (df[distr_col].astype(str).str.strip() != '')))
                    pct_date_loc = mask_date_loc.sum() / registros_iniciales * 100 if registros_iniciales > 0 else 0

                    pct_insumos_gt0 = (total_insumos > 0).sum() / registros_iniciales * 100 if registros_iniciales > 0 else 0

                    print("\nDimensión: Completitud")
                    print(f"% de registros con todos los campos obligatorios: {pct_all_fields:.1f}%")
                    print(f"% de registros con al menos fecha y ubicación: {pct_date_loc:.1f}%")
                    print(f"% de registros con insumos > 0: {pct_insumos_gt0:.1f}%")
                except Exception:
                    pass

                # Métricas de exactitud / consistencia básica
                try:
                    dept_variants = len(df[dept_col].fillna('').astype(str).unique()) if dept_col in df.columns else 0
                    distr_variants = len(df[distr_col].fillna('').astype(str).unique()) if distr_col in df.columns else 0
                    evento_variants = len(df[evento_col].fillna('').astype(str).unique()) if evento_col in df.columns else 0

                    # valores numéricos fuera de rango: negativos o NaN in insumos
                    neg_mask = (total_insumos < 0).sum()
                    pct_neg = neg_mask / registros_iniciales * 100 if registros_iniciales > 0 else 0

                    print("\nDimensión: Exactitud")
                    print(f"Consistencia de nombres de departamentos: {'MUY BAJA' if dept_variants > len(self.departamento_orden) * 3 else 'BAJA' if dept_variants > len(self.departamento_orden) else 'OK'} ({dept_variants} variantes para {len(self.departamento_orden)} departamentos)")
                    print(f"Consistencia de nombres de distritos: {'BAJA' if distr_variants > len(self.todos_distritos_validos) else 'OK'} ({distr_variants} variantes para ~{len(self.todos_distritos_validos)} distritos)")
                    print(f"Consistencia de nombres de eventos: {'MUY BAJA' if evento_variants > 50 else 'OK'} ({evento_variants} variantes)")
                    print(f"Valores numéricos fuera de rangos lógicos: {'POCOS' if pct_neg < 1 else 'ATENCIÓN'} (<{pct_neg:.1f}%)")
                except Exception:
                    pass

                # Consistencia: departamento vs distrito mapping (usa DISTRITOS_POR_DEPARTAMENTO)
                try:
                    mismatch_count = 0
                    mismatches_samples = []
                    # Consideramos solo filas donde ambos campos están presentes y no vacíos
                    if dept_col in df.columns and distr_col in df.columns:
                        mask_both = df[dept_col].notna() & df[distr_col].notna() & (df[dept_col].astype(str).str.strip() != '') & (df[distr_col].astype(str).str.strip() != '')
                        rows_with_both = int(mask_both.sum())
                        if rows_with_both > 0:
                            for idx, row in df.loc[mask_both, [dept_col, distr_col]].iterrows():
                                d_actual = row.get(distr_col, '')
                                dept_actual = row.get(dept_col, '')
                                d_norm = self._norm_str(d_actual)
                                # buscamos el departamento oficial asignado al distrito (si existe)
                                mapped_dept = self.distrito_a_departamento_norm.get(d_norm)
                                if mapped_dept:
                                    if self._norm_str(dept_actual) != self._norm_str(mapped_dept):
                                        mismatch_count += 1
                                        # guardamos una muestra para diagnóstico
                                        if len(mismatches_samples) < 5:
                                            mismatches_samples.append((idx, d_actual, dept_actual, mapped_dept))
                    else:
                        rows_with_both = 0

                    # Imprimir resumen con dos denominadores: total inicial y filas con ambos campos
                    pct_mismatch_total = mismatch_count / registros_iniciales * 100 if registros_iniciales > 0 else 0
                    pct_mismatch_both = (mismatch_count / rows_with_both * 100) if rows_with_both > 0 else 0

                    print("\nDimensión: Consistencia (Departamento <-> Distrito)")
                    print(f"Inconsistencias detectadas: {mismatch_count:,} casos | {pct_mismatch_total:.1f}% del total inicial | {pct_mismatch_both:.1f}% de filas con ambos campos ({rows_with_both:,} filas)")
                    if mismatches_samples:
                        print("  Ejemplos de inconsistencias (índice, distrito_raw, depto_raw, depto_esperado):")
                        for s in mismatches_samples:
                            print(f"   - {s[0]}: Distrito='{s[1]}', Depto='{s[2]}' -> Esperado='{s[3]}'")
                    # Añadimos aquí la regla de fecha como parte de la dimensión de consistencia
                    try:
                        if date_col in df.columns:
                            parsed_dates_local = pd.to_datetime(df[date_col], errors='coerce', dayfirst=True, infer_datetime_format=True)
                            valid_year_mask_local = (parsed_dates_local.dt.year >= 2018) & (parsed_dates_local.dt.year <= 2023)
                            fecha_bad_count_local = int((~valid_year_mask_local).sum())
                            pct_fecha_ok_local = (registros_iniciales - fecha_bad_count_local) / registros_iniciales * 100 if registros_iniciales > 0 else 0
                            print(f"Fecha debe estar en rango 2018-2023 -> {pct_fecha_ok_local:.1f}% -> {fecha_bad_count_local:,} ({(100-pct_fecha_ok_local):.1f}% incumplen)")
                    except Exception:
                        pass
                except Exception:
                    pass

                    if date_col in df.columns:
                        parsed = pd.to_datetime(df[date_col], errors='coerce', dayfirst=True, infer_datetime_format=True)
                        # intentamos recuperar fechas inválidas (serial Excel y AÑO/MES)
                        def try_excel_serial_local(val):
                            try:
                                if pd.isna(val):
                                    return None
                                if isinstance(val, (int, float)) and val > 1000:
                                    return (pd.to_datetime('1899-12-30') + pd.to_timedelta(int(val), unit='D'))
                                if isinstance(val, str) and re.fullmatch(r"\d+", val.strip()):
                                    iv = int(val.strip())
                                    if iv > 1000:
                                        return (pd.to_datetime('1899-12-30') + pd.to_timedelta(iv, unit='D'))
                            except Exception:
                                return None
                            return None

                        parsed_after = parsed.copy()
                        mask_nat = parsed_after.isna()
                        if mask_nat.any():
                            for idx in df[mask_nat].index:
                                orig = df.at[idx, date_col]
                                alt = try_excel_serial_local(orig)
                                if alt is not None:
                                    parsed_after.at[idx] = alt

                        # Intentar reconstruir desde AÑO/MES si sigue faltando
                        mask_nat = parsed_after.isna()
                        year_cols = [c for c in df.columns if c.upper() in ('AÑO', 'ANO', 'ANIO', 'YEAR')]
                        month_cols = [c for c in df.columns if c.upper() in ('MES', 'MONTH', 'MES_NOMBRE')]
                        if mask_nat.any() and year_cols and month_cols:
                            for idx in df[mask_nat].index:
                                try:
                                    y = int(df.at[idx, year_cols[0]])
                                    m = int(df.at[idx, month_cols[0]])
                                    if y > 1900 and 1 <= m <= 12:
                                        parsed_after.at[idx] = pd.Timestamp(year=y, month=m, day=1)
                                except Exception:
                                    continue
                    pass

        except Exception:
            # no romper el pipeline si hay algún fallo en este bloque analítico
            pass
        # --- Dimensión: Validez - usa los valores tal como entran al pipeline ---
        try:
            print("\nDimensión: Validez")
            # 1) Regla: al menos un insumo > 0 (detección por keywords en nombres de columna)
            insumo_keywords_init = ['KIT', 'CHAPA', 'COLCHON', 'COLCHONES', 'FRAZ', 'TERCIAD', 'PUNTA', 'CARPA']
            insumo_cols_init = [c for c in df.columns if any(k in c.upper() for k in insumo_keywords_init)]
            if insumo_cols_init:
                total_insumos_init = df[insumo_cols_init].apply(lambda s: pd.to_numeric(s, errors='coerce').fillna(0)).sum(axis=1)
            else:
                total_insumos_init = pd.Series(0, index=df.index)

            ok_insumos = int((total_insumos_init > 0).sum())
            fail_insumos = registros_iniciales - ok_insumos
            pct_ok_insumos = ok_insumos / registros_iniciales * 100 if registros_iniciales > 0 else 0

            # 2) Regla: departamento debe ser uno de los oficiales
            dept_valid_count = 0
            dept_official_norm = {self._norm_str(k) for k in self.departamento_orden.keys()}
            if dept_col in df.columns:
                # máscara de filas con valor no vacío
                non_empty_mask = df[dept_col].notna() & (df[dept_col].astype(str).str.strip() != '')
                # para cada valor no vacío verificamos si su forma normalizada está en la lista oficial
                try:
                    dept_norm_series = df.loc[non_empty_mask, dept_col].astype(str).apply(self._norm_str)
                    dept_valid_count = int(dept_norm_series.isin(dept_official_norm).sum())
                except Exception:
                    # fallback conservador: contar 0 válidos si algo falla
                    dept_valid_count = 0
            fail_dept = registros_iniciales - dept_valid_count
            pct_dept_ok = dept_valid_count / registros_iniciales * 100 if registros_iniciales > 0 else 0

            # 3) Regla: fecha debe estar en rango 2018-2023
            fecha_bad_count = 0
            if date_col is not None:
                parsed_init = pd.to_datetime(df[date_col], errors='coerce', dayfirst=True, infer_datetime_format=True)
                valid_year_mask = (parsed_init.dt.year >= 2018) & (parsed_init.dt.year <= 2023)
                fecha_bad_count = int((~valid_year_mask).sum())
            pct_fecha_ok = (registros_iniciales - fecha_bad_count) / registros_iniciales * 100 if registros_iniciales > 0 else 0

            # 4) Regla: evento no puede estar vacío
            evento_non_empty = 0
            if evento_col in df.columns:
                evento_non_empty = int(df[evento_col].notna().astype(bool).sum())
            fail_evento = registros_iniciales - evento_non_empty
            pct_evento_ok = evento_non_empty / registros_iniciales * 100 if registros_iniciales > 0 else 0

            # Contexto: primeros 10 registros que NO tienen insumos (TOTAL_INS<=0) en la vista inicial
            try:
                cols_show = []
                if evento_col in df.columns:
                    cols_show.append(evento_col)
                if dept_col in df.columns:
                    cols_show.append(dept_col)
                if date_col in df.columns:
                    cols_show.append(date_col)

                # si no detectamos columnas para mostrar, saltamos
                if cols_show:
                    no_insumos_mask = total_insumos_init <= 0
                    if no_insumos_mask.any():
                        sample_no_insumos = df.loc[no_insumos_mask, cols_show].head(50)
                        print("\nPrimeros 10 registros SIN insumos (campos: Evento, Departamento, Fecha si existen):")
                        # imprimimos con índice para rastrear filas originales
                        try:
                            print(sample_no_insumos.to_string(index=True))
                        except Exception:
                            # fallback sencillo línea a línea
                            for i, row in sample_no_insumos.iterrows():
                                vals = [str(row[c]) for c in cols_show]
                                print(f"- {i}: {' | '.join(vals)}")
                    else:
                        print("\nNo se encontraron registros sin insumos en la vista inicial.")
                else:
                    print("\nNo hay columnas Evento/Departamento/Fecha detectadas para mostrar contexto de registros sin insumos.")
            except Exception:
                pass

            # Imprimir resumen compacto (una línea por regla, formato: Regla -> %cumplimiento -> N (X% incumplen))
            print("Regla de Negocio | Cumplimiento | Registros Incumpliendo")
            print(f"Al menos un insumo debe ser > 0 -> {pct_ok_insumos:.1f}% -> {fail_insumos:,} ({(100-pct_ok_insumos):.1f}% incumplen)")
            print(f"Departamento debe ser uno de los oficiales -> {pct_dept_ok:.1f}% -> {fail_dept:,} ({(100-pct_dept_ok):.1f}% incumplen)")
            print(f"Fecha debe estar en rango 2018-2023 -> {pct_fecha_ok:.1f}% -> {fecha_bad_count:,} ({(100-pct_fecha_ok):.1f}% incumplen)")
            print(f"Evento no puede estar vacío -> {pct_evento_ok:.1f}% -> {fail_evento:,} ({(100-pct_evento_ok):.1f}% incumplen)")
        except Exception:
            pass

        # Normalizamos nombres de columnas para consistencia
        df.columns = [col.upper().replace(' ', '_') for col in df.columns]

        # 1. Normalizamos todas las ubicaciones (¡ahora mejorada con JSON!)
        df = self.normalize_locations(df)

        # 2. Estandarizamos eventos (antes de inferir)
        if 'EVENTO' in df.columns:
            df['EVENTO'] = df['EVENTO'].apply(self.estandarizar_evento_robusto)

        # 3. Inferimos eventos cuando no están especificados
        print("🔍 Aplicando inferencia de eventos basada en recursos...")
        eventos_inferidos = 0
        
        # Preparamos columnas temporales para cálculos eficientes
        insumos_cols_map = {
            'KIT_A': 'KIT_A', 'KIT_B': 'KIT_B',
            'CHAPA_FIBROCEMENTO': 'CHAPA_FIBROCEMENTO', 'CHAPA_ZINC': 'CHAPA_ZINC',
            'COLCHONES': 'COLCHONES', 'FRAZADAS': 'FRAZADAS', 
            'TERCIADAS': 'TERCIADAS', 'PUNTALES': 'PUNTALES', 'CARPAS_PLASTICAS': 'CARPAS_PLASTICAS'
        }
        
        # Mapeamos columnas originales a versiones temporales limpias
        temp_col_map = {}
        for final_col, _ in insumos_cols_map.items():
            found_col = next((col for col in df.columns if col == final_col), None)
            if found_col:
                df[f'{final_col}_TEMP'] = df[found_col].apply(self.limpiar_numero)
                temp_col_map[final_col] = f'{final_col}_TEMP'
            else:
                df[f'{final_col}_TEMP'] = 0
                temp_col_map[final_col] = f'{final_col}_TEMP'

        # Aplicamos inferencia a cada registro
        for idx, row in df.iterrows():
            temp_row = row.to_dict()
            for final_col, temp_col in temp_col_map.items():
                temp_row[final_col.replace('_', ' ')] = row[temp_col]

            evento_original = row['EVENTO']
            evento_inferido = self.post_process_eventos_with_aids(temp_row)

            if evento_original != evento_inferido:
                eventos_inferidos += 1
                df.at[idx, 'EVENTO'] = evento_inferido

        print(f"  Eventos inferidos/ajustados: {eventos_inferidos}")
        # Distribution prints for EVENTO (pre/post inference) suppressed to reduce noisy console output
        
        # Limpiamos columnas temporales
        cols_to_drop = [f'{col}_TEMP' for col in insumos_cols_map.keys() if f'{col}_TEMP' in df.columns]
        df = df.drop(columns=cols_to_drop, errors='ignore')

        # 4. Filtramos registros que no deben ir al Data Warehouse
        registros_antes = len(df)
        print(f"  Registros antes de eliminación: {registros_antes}")

        # Limpiamos números en columnas de insumos
        for col in insumos_cols_map.keys():
            df[col] = df.get(col, pd.Series([0] * len(df), index=df.index)).apply(self.limpiar_numero)

        # Calculamos total de insumos por registro
        insumo_cols = list(insumos_cols_map.keys())
        df['TOTAL_INSUMOS'] = df[insumo_cols].sum(axis=1)

        # 4a. Eliminamos registros de preposicionamiento
        registros_eliminados_prepos = int((df['EVENTO'] == 'ELIMINAR_REGISTRO').sum())
        df_limpio = df[df['EVENTO'] != 'ELIMINAR_REGISTRO'].copy()
        print(f"  Registros marcados ELIMINAR_REGISTRO: {registros_eliminados_prepos}")

        # 4b. Eliminamos registros sin insumos
        registros_sin_insumos = int((df_limpio['TOTAL_INSUMOS'] <= 0).sum()) if 'TOTAL_INSUMOS' in df_limpio.columns else 0
        df_limpio = df_limpio[df_limpio['TOTAL_INSUMOS'] > 0]
        registros_eliminados_cero = registros_sin_insumos
        print(f"  Registros sin insumos (TOTAL_INSUMOS<=0): {registros_sin_insumos}")

        df = df_limpio.drop(columns=['TOTAL_INSUMOS'], errors='ignore')

        print(f"  Registros eliminados (Preposicionamiento): {registros_eliminados_prepos:,}")
        print(f"  Registros eliminados (Sin insumos): {registros_eliminados_cero:,}")
        print(f"  Registros restantes: {len(df):,}")

        # 5. Creamos características adicionales para análisis
        df = self.feature_engineering_basico(df)

        # 6. Aseguramos el formato final para el Data Warehouse
        df = self.estandarizacion_final_columnas(df)

        return df

    def feature_engineering_basico(self, df):
        """Crea características adicionales que facilitan el análisis.
        
        Estas características nuevas ayudan a hacer agrupamientos y filtros
        en los dashboards y reportes del Data Warehouse.
        """
        # Buscamos la columna de fecha (puede tener diferentes nombres)
        fecha_cols = [col for col in df.columns if 'FECHA' in col.upper()]
        if fecha_cols:
            col_fecha = fecha_cols[0]

            # Intentamos parsear las fechas de manera flexible
            df[col_fecha] = pd.to_datetime(df[col_fecha], errors='coerce', dayfirst=True, infer_datetime_format=True)

            # Si hay fechas que no pudimos parsear, intentamos estrategias alternativas
            n_invalid_fecha = int(df[col_fecha].isna().sum())
            if n_invalid_fecha > 0:
                print(f"  Nota: {n_invalid_fecha} filas inicialmente no parsearon como fecha. Aplicando heurísticas de recuperación...")

                # Estrategia A: Detectar números de Excel (fechas como números seriales)
                def try_excel_serial(val):
                    try:
                        if pd.isna(val):
                            return None
                        if isinstance(val, (int, float)) and val > 1000:
                            return (pd.to_datetime('1899-12-30') + pd.to_timedelta(int(val), unit='D'))
                        if isinstance(val, str) and re.fullmatch(r"\d+", val.strip()):
                            iv = int(val.strip())
                            if iv > 1000:
                                return (pd.to_datetime('1899-12-30') + pd.to_timedelta(iv, unit='D'))
                    except Exception:
                        return None
                    return None

                mask_nat = df[col_fecha].isna()
                if mask_nat.any():
                    recovered = 0
                    for idx in df[mask_nat].index:
                        orig = df.at[idx, col_fecha]
                        alt = try_excel_serial(orig)
                        if alt is not None:
                            df.at[idx, col_fecha] = alt
                            recovered += 1
                    if recovered > 0:
                        print(f"    Recuperadas {recovered} fechas desde seriales de Excel.")

                # Estrategia B: Construir fecha desde columnas de año y mes separadas
                mask_nat = df[col_fecha].isna()
                year_cols = [c for c in df.columns if c.upper() in ('AÑO', 'ANO', 'ANIO', 'YEAR')]
                month_cols = [c for c in df.columns if c.upper() in ('MES', 'MONTH', 'MES_NOMBRE')]
                if mask_nat.any() and year_cols and month_cols:
                    recovered_ym = 0
                    for idx in df[mask_nat].index:
                        try:
                            y = int(df.at[idx, year_cols[0]])
                            m = int(df.at[idx, month_cols[0]])
                            if y > 1900 and 1 <= m <= 12:
                                df.at[idx, col_fecha] = pd.Timestamp(year=y, month=m, day=1)
                                recovered_ym += 1
                        except Exception:
                            continue
                    if recovered_ym > 0:
                        print(f"    Reconstruidas {recovered_ym} fechas a partir de columnas AÑO/MES.")

                # Reportamos cuántas fechas pudimos recuperar
                n_invalid_fecha_after = int(df[col_fecha].isna().sum())
                n_recovered_total = n_invalid_fecha - n_invalid_fecha_after
                if n_recovered_total > 0:
                    print(f"  Heurísticas recuperaron {n_recovered_total} fechas. {n_invalid_fecha_after} siguen inválidas.")

                # Mostramos ejemplos de fechas que no pudimos parsear
                if n_invalid_fecha_after > 0:
                    sample_invalid = df[df[col_fecha].isna()].head(10)
                    print("  Ejemplos de valores de FECHA no parseados (primeros 10):")
                    for i, r in sample_invalid.iterrows():
                        print(f"    idx={i} valor_original={repr(r.get(col_fecha))}")

            # Eliminamos filas sin fecha válida (no se pueden analizar temporalmente)
            final_invalid = int(df[col_fecha].isna().sum())
            if final_invalid > 0:
                print(f"  Advertencia: {final_invalid} filas siguen sin FECHA válida y serán descartadas antes de la carga.")
                df = df[df[col_fecha].notna()].copy()

            # Creamos columnas de año y mes para agrupamientos
            df['AÑO'] = df[col_fecha].dt.year
            df['MES'] = df[col_fecha].dt.month

            # Filtramos fechas con años no realistas
            mask_invalid_year = df['AÑO'] <= 1900
            n_invalid_years = int(mask_invalid_year.sum())
            if n_invalid_years > 0:
                print(f"  Advertencia: {n_invalid_years} filas tienen AÑO <= 1900 y serán descartadas (fechas inválidas).")
                df = df[~mask_invalid_year].copy()

        # Agregamos orden de departamento para visualizaciones consistentes
        if 'DEPARTAMENTO' in df.columns:
            df['ORDEN_DEPARTAMENTO'] = df['DEPARTAMENTO'].map(self.departamento_orden).fillna(0).astype(int)

        return df

    def estandarizacion_final_columnas(self, df):
        """Asegura que los datos tengan el formato exacto que espera el Data Warehouse.
        
        Esta función garantiza que todas las columnas necesarias estén presentes
        y en el formato correcto, listas para ser cargadas en las tablas del DW.
        """
        # Definimos exactamente qué columnas y formatos espera el Data Warehouse
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

        # Construimos el DataFrame final con todas las columnas esperadas
        df_final = pd.DataFrame()
        for col, dtype in columnas_finales.items():
            # Si la columna no existe, la creamos con valores por defecto
            if col not in df.columns:
                if dtype in ['int64', 'float64']:
                    df_final[col] = 0
                else:
                    df_final[col] = 'SIN ESPECIFICAR'
            else:
                # Si existe, la copiamos y aplicamos la limpieza según el tipo
                if dtype in ['int64', 'float64']:
                    df_final[col] = df[col].apply(self.limpiar_numero)
                elif dtype == 'object':
                    df_final[col] = df[col].apply(self.limpiar_texto)
                elif dtype == 'datetime64[ns]':
                    df_final[col] = pd.to_datetime(df[col], errors='coerce')

        return df_final.astype(columnas_finales, errors='ignore')

    def verificacion_final(self, df):
        """Revisa que todo esté correcto antes de enviar los datos al Data Warehouse.
        
        Hace una última verificación para asegurar que los datos estén limpios
        y consistentes, listos para ser usados en análisis y reportes.
        """
        print("\n🔍 VERIFICACIÓN FINAL:")

        # Verificamos departamentos
        if 'DEPARTAMENTO' in df.columns:
            deptos_finales = df['DEPARTAMENTO'].unique()
            deptos_esperados = set(self.departamento_orden.keys())
            deptos_extra = set(deptos_finales) - deptos_esperados
            
            print(f"✅ DEPARTAMENTOS FINALES: {len(deptos_finales)}")
            print(f"📋 Lista: {sorted(list(deptos_finales))}")

            if deptos_extra:
                print(f"❌ DEPARTAMENTOS EXTRA/NO ESPERADOS: {deptos_extra}")
            else:
                print("🎉 ¡Todos los departamentos están correctamente estandarizados o son esperados!")

        # Verificamos distritos
        if 'DISTRITO' in df.columns:
            distritos_finales = df['DISTRITO'].unique()
            distritos_validos = len([d for d in distritos_finales if d in self.todos_distritos_validos])
            print(f"✅ DISTRITOS FINALES: {len(distritos_finales)}")
            print(f"📋 Distritos válidos: {distritos_validos}/{len(distritos_finales)}")
            
            # Mostramos distritos no válidos para investigación
            distritos_no_validos = [d for d in distritos_finales if d not in self.todos_distritos_validos and d != 'SIN ESPECIFICAR']
            if distritos_no_validos:
                print(f"❌ DISTRITOS NO VÁLIDOS: {distritos_no_validos}")

        # Verificamos localidades
        if 'LOCALIDAD' in df.columns:
            localidades_finales = df['LOCALIDAD'].value_counts()
            print(f"✅ LOCALIDADES FINALES: {len(localidades_finales)}")
            print("📊 Top 10 localidades más comunes:")
            for localidad, count in localidades_finales.head(10).items():
                print(f"  - {localidad}: {count}")

        # Verificamos eventos
        if 'EVENTO' in df.columns:
            eventos_finales = df['EVENTO'].value_counts()
            print(f"✅ EVENTOS FINALES: {len(eventos_finales)}")
            print("📊 Distribución Top 10:")
            for evento, count in eventos_finales.head(10).items():
                print(f"  - {evento}: {count}")

        return df