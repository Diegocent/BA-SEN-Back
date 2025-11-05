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
import unicodedata
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
            'PARAGUARI - GUAIRA': 'PARAGUARÍ', 'Paraguari - Guaira': 'PARAGUARÍ',
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

        # Mapeo de distritos que a veces fueron escritos en la columna DEPARTAMENTO
        # (registradores pusieron el distrito en vez del departamento).
        # Se usan claves en mayúsculas para comparar contra el texto limpiado.
        self.distrito_a_departamento = {
            'ASUNCIÓN': 'CAPITAL', 'ASUNCION': 'CAPITAL',
            'LIMPIO': 'CENTRAL', 'MARIANO ROQUE ALONSO': 'CENTRAL', 'ÑEMBY': 'CENTRAL',
            'SAN LORENZO': 'CENTRAL', 'LAMBARÉ': 'CENTRAL', 'FERNANDO DE LA MORA': 'CENTRAL',
            'VILLA ELISA': 'CENTRAL', 'LUQUE': 'CENTRAL', 'CAPIATÁ': 'CENTRAL', 'CAPIATA': 'CENTRAL',
            'ITA': 'CENTRAL', 'ITAUGUA': 'CENTRAL', 'VILLARRICA': 'GUAIRÁ', 'CORONEL OVIEDO': 'CAAGUAZÚ',
            'CAACUPÉ': 'CORDILLERA', 'CAACUPE': 'CORDILLERA'
        }

        # Mapeo puntual de localidades a su distrito y departamento.
        # Este mapa se usa para inferir DISTRITO/DEPARTAMENTO cuando la fila
        # sólo tiene LOCALIDAD o cuando los valores están en la columna equivocada.
        self.localidad_map = {
            'ASUNCION': {'distrito': 'ASUNCIÓN', 'departamento': 'CAPITAL'},
            'BAÑADO SUR': {'distrito': 'ASUNCIÓN', 'departamento': 'CAPITAL'},
            'BARRIO ROBERTO L. PETTIT': {'distrito': 'ASUNCIÓN', 'departamento': 'CAPITAL'},
            'BARRIO SAN FRANCISCO': {'distrito': 'ASUNCIÓN', 'departamento': 'CAPITAL'},
            'CATEURA': {'distrito': 'ASUNCIÓN', 'departamento': 'CAPITAL'},
            'CHACARITA - PARQUE CABALLERO': {'distrito': 'ASUNCIÓN', 'departamento': 'CAPITAL'},
            'PUERTO BOTANICO': {'distrito': 'ASUNCIÓN', 'departamento': 'CAPITAL'},
            'RICARDO BRUGADA - CHACARITA': {'distrito': 'ASUNCIÓN', 'departamento': 'CAPITAL'},
            'SAJONIA': {'distrito': 'ASUNCIÓN', 'departamento': 'CAPITAL'},
            'SANTA ANA': {'distrito': 'ASUNCIÓN', 'departamento': 'CAPITAL'},
            'TABLADA NUEVA': {'distrito': 'ASUNCIÓN', 'departamento': 'CAPITAL'},
            'VIRGEN DE FATIMA': {'distrito': 'ASUNCIÓN', 'departamento': 'CAPITAL'},
            'ZEBALLOS CUE': {'distrito': 'ASUNCIÓN', 'departamento': 'CAPITAL'},
            'CAACUPEMI - ZEBALLOS CUE': {'distrito': 'ASUNCIÓN', 'departamento': 'CAPITAL'},
            'NANAWA': {'distrito': 'NANAWA', 'departamento': 'PDTE. HAYES'},
            'GRAL. BRUGUEZ': {'distrito': 'GENERAL BRUGUEZ', 'departamento': 'PDTE. HAYES'},
            'POZO COLORADO': {'distrito': 'TTE. IRALA FERNÁNDEZ', 'departamento': 'PDTE. HAYES'},
            'SAN FERNANDO': {'distrito': 'TTE. IRALA FERNÁNDEZ', 'departamento': 'PDTE. HAYES'},
            'MAYOR MARTINEZ': {'distrito': 'MAYOR JOSÉ J. MARTÍNEZ', 'departamento': 'ÑEEMBUCÚ'},
            'PILAR': {'distrito': 'PILAR', 'departamento': 'ÑEEMBUCÚ'},
            'VILLA FRANCA': {'distrito': 'VILLA FRANCA', 'departamento': 'ÑEEMBUCÚ'},
            'VILLA OLIVA': {'distrito': 'VILLA OLIVA', 'departamento': 'ÑEEMBUCÚ'},
            'CERRITO': {'distrito': 'CERRITO', 'departamento': 'ÑEEMBUCÚ'},
            'CIUDAD DEL ESTE': {'distrito': 'CIUDAD DEL ESTE', 'departamento': 'ALTO PARANÁ'},
            'ENCARNACION': {'distrito': 'ENCARNACIÓN', 'departamento': 'ITAPÚA'},
            'MARIA AUXILIADORA': {'distrito': 'TOMÁS ROMERO PEREIRA', 'departamento': 'ITAPÚA'},
            'ITAPUA POTY': {'distrito': 'ITAPÚA POTY', 'departamento': 'ITAPÚA'},
            'ANACONDA': {'distrito': 'ENCARNACIÓN', 'departamento': 'ITAPÚA'},
            'LA PAZ': {'distrito': 'LA PAZ', 'departamento': 'ITAPÚA'},
            'AYOLAS': {'distrito': 'AYOLAS', 'departamento': 'MISIONES'},
            'SAN JUAN': {'distrito': 'SAN JUAN BAUTISTA', 'departamento': 'MISIONES'},
            'ACAHAY': {'distrito': 'ACAHAY', 'departamento': 'PARAGUARÍ'},
            'PARAGUARI': {'distrito': 'PARAGUARÍ', 'departamento': 'PARAGUARÍ'},
            'TEBICUARYMI': {'distrito': 'TEBUICUARYMI', 'departamento': 'PARAGUARÍ'},
            'YBYTYMI': {'distrito': 'YBYTYMÍ', 'departamento': 'PARAGUARÍ'},
            'AREGUA': {'distrito': 'AREGUÁ', 'departamento': 'CENTRAL'},
            'CAPIATA': {'distrito': 'CAPIATÁ', 'departamento': 'CENTRAL'},
            'FERNANDO DE LA MORA': {'distrito': 'FERNANDO DE LA MORA', 'departamento': 'CENTRAL'},
            'GUARAMBARE': {'distrito': 'GUARAMBARÉ', 'departamento': 'CENTRAL'},
            'ITA': {'distrito': 'ITÁ', 'departamento': 'CENTRAL'},
            'LAMBARE': {'distrito': 'LAMBARÉ', 'departamento': 'CENTRAL'},
            'LIMPIO': {'distrito': 'LIMPIO', 'departamento': 'CENTRAL'},
            'LUQUE': {'distrito': 'LUQUE', 'departamento': 'CENTRAL'},
            'MARIANO R. ALONSO': {'distrito': 'MARIANO ROQUE ALONSO', 'departamento': 'CENTRAL'},
            'ÑEMBY': {'distrito': 'ÑEMBY', 'departamento': 'CENTRAL'},
            'SAN LORENZO': {'distrito': 'SAN LORENZO', 'departamento': 'CENTRAL'},
            'VILLET A': {'distrito': 'VILLET A', 'departamento': 'CENTRAL'},
            'VILLA ELISA': {'distrito': 'VILLA ELISA', 'departamento': 'CENTRAL'},
            'REDUCTO': {'distrito': 'SAN LORENZO', 'departamento': 'CENTRAL'},
            'TOBATI': {'distrito': 'TOBATÍ', 'departamento': 'CORDILLERA'},
            'CARAYAO': {'distrito': 'CARAYAÓ', 'departamento': 'CAAGUAZÚ'},
            'RI 3 CORRALES': {'distrito': 'R.I. TRES CORRALES', 'departamento': 'CAAGUAZÚ'},
            'YHU': {'distrito': 'YHÚ', 'departamento': 'CAAGUAZÚ'},
            'BELLA VISTA NORTE': {'distrito': 'BELLA VISTA', 'departamento': 'AMAMBAY'},
            'RESQUIN': {'distrito': 'GENERAL ISIDORO RESQUÍN', 'departamento': 'SAN PEDRO'},
            'SANTA ROSA DEL AGUARAY': {'distrito': 'SANTA ROSA DEL AGUARAY', 'departamento': 'SAN PEDRO'},
            'ANTEQUERA': {'distrito': 'ANTEQUERA', 'departamento': 'SAN PEDRO'},
            'LIMA': {'distrito': 'LIMA', 'departamento': 'SAN PEDRO'},
            'SAN ESTANISLAO': {'distrito': 'SAN ESTANISLAO', 'departamento': 'SAN PEDRO'},
            'SAN PEDRO DEL YCUAMANDYJU': {'distrito': 'SAN PEDRO DEL YCUAMANDIYÚ', 'departamento': 'SAN PEDRO'},
            'UNION': {'distrito': 'UNIÓN', 'departamento': 'SAN PEDRO'},
            '25 DE DICIEMBRE': {'distrito': '25 DE DICIEMBRE', 'departamento': 'SAN PEDRO'},
            'PASO BARRETO': {'distrito': 'PASO BARRETO', 'departamento': 'CONCEPCIÓN'},
            'VALLEMI': {'distrito': 'SAN LÁZARO', 'departamento': 'CONCEPCIÓN'},
            'FUERTE OLIMPO': {'distrito': 'FUERTE OLIMPO', 'departamento': 'ALTO PARAGUAY'},
            'BAHIA NEGRA': {'distrito': 'BAHÍA NEGRA', 'departamento': 'ALTO PARAGUAY'},
            'CARMELO PERALTA': {'distrito': 'CAPITÁN CARMELO PERALTA', 'departamento': 'ALTO PARAGUAY'},
            'PUERTO CASADO': {'distrito': 'PUERTO CASADO', 'departamento': 'ALTO PARAGUAY'},
            'PUERTO PINASCO': {'distrito': 'PUERTO PINASCO', 'departamento': 'ALTO PARAGUAY'},
            'LOMA PLATA': {'distrito': 'LOMA PLATA', 'departamento': 'BOQUERON'},
            'BOQUERON': {'distrito': 'BOQUERÓN', 'departamento': 'BOQUERON'},
            'MCAL. ESTIGARRIBIA': {'distrito': 'MARISCAL JOSÉ FÉLIX ESTIGARRIBIA', 'departamento': 'BOQUERON'},
            'PEDRO P. PEÑA': {'distrito': 'PEDRO P. PEÑA', 'departamento': 'BOQUERON'},
            'COLONIAS ARMONIA': {'distrito': 'LOMA PLATA', 'departamento': 'BOQUERON'},
            'ASENTAMIENTOS NICHATOCHIT': {'distrito': 'MARISCAL ESTIGARRIBIA', 'departamento': 'BOQUERON'},
            'YRYBUCUA': {'distrito': 'YRYBUCUÁ', 'departamento': 'CANINDEYÚ'},
            'CRUCE LIBERACION': {'distrito': 'CRUCE LIBERACIÓN', 'departamento': 'CANINDEYÚ'},
            'YATAITY DEL NORTE': {'distrito': 'YATAITY DEL NORTE', 'departamento': 'GUAIRÁ'},
            'LA ROSA KUE': {'distrito': 'MINGA GUAZÚ', 'departamento': 'ALTO PARANÁ'},
            'LA ESPERANZA': {'distrito': 'MAYOR OTAÑO / NARANJAL', 'departamento': 'ITAPÚA / ALTO PARANÁ'},
            'NUEVA PROMESA': {'distrito': 'FILADELFIA / LOMA PLATA', 'departamento': 'BOQUERON'},
        }

        # --- Preparar versiones normalizadas (sin acentos, en mayúsculas) de los mapas
        # Esto permite reconocer entradas con/ sin acentos o con errores de tildes.
        def _norm_str(s):
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

        # Normalizar estandarizacion_dept
        self.estandarizacion_dept_norm = {}
        for k, v in self.estandarizacion_dept.items():
            self.estandarizacion_dept_norm[_norm_str(k)] = v

        # Normalizar distrito->departamento
        self.distrito_a_departamento_norm = {}
        for k, v in self.distrito_a_departamento.items():
            self.distrito_a_departamento_norm[_norm_str(k)] = v

        # Normalizar localidad_map
        self.localidad_map_norm = {}
        for k, v in self.localidad_map.items():
            self.localidad_map_norm[_norm_str(k)] = {'distrito': v.get('distrito'), 'departamento': v.get('departamento')}

        # Construir un mapa canónico de localidades (normalized -> canonical original)
        self.localidad_canonical_map = {}
        for k in self.localidad_map.keys():
            self.localidad_canonical_map[_norm_str(k)] = k

        # Construir un mapa canónico de distritos (normalized -> canonical with accents)
        self.distrito_canonical_map = {}
        # Prefer valores 'distrito' que aparecen en localidad_map como fuente canónica
        for v in self.localidad_map.values():
            d = v.get('distrito')
            if d:
                self.distrito_canonical_map[_norm_str(d)] = d
        # Asegurar algunos casos explícitos si faltaran
        self.distrito_canonical_map.setdefault(_norm_str('ASUNCIÓN'), 'ASUNCIÓN')
        self.distrito_canonical_map.setdefault(_norm_str('ASUNCION'), 'ASUNCIÓN')

        # Diccionario de estandarización explícita de distritos (variantes -> canonical)
        self.estandarizacion_distrito = {
            'Asuncion': 'ASUNCIÓN',
            'Asunciòn': 'ASUNCIÓN',
            'Asunción': 'ASUNCIÓN',
            'Zeballos Cue': 'ZEBALLOS CUE',
            'Encarnacion': 'ENCARNACIÓN',
            'Encarnaciòn': 'ENCARNACIÓN',
            'Mariano R. Alonso': 'MARIANO ROQUE ALONSO',
            'Gral. Bruguez': 'GENERAL BRUGUEZ',
            'Caaguazu': 'CAAGUAZÚ',
            'Caazapa - Ava´i': 'CAAZAPÁ - AVAÍ',
            'Caazapa - Guaira': 'CAAZAPÁ - GUAIRÁ',
            'San Juan': 'SAN JUAN BAUTISTA',
            'Tebicuarymi': 'TEBICUARYMÍ',
            'Ybytymi': 'YBYTYMÍ',
            'Yguazu': 'YGUASÚ',
            'Fuerte Olimpo': 'FUERTE OLIMPO',
            'Bahia Negra': 'BAHÍA NEGRA',
            '1ra. DI': '1RA. DI',
            '25 de Diciembre': '25 DE DICIEMBRE',
            'Acahay': 'ACAHAY',
            'Alto Parana': 'ALTO PARANÁ',
            'Altos y Loma Grande': 'ALTOS Y LOMA GRANDE',
            'Antequera': 'ANTEQUERA',
            'Aregua': 'AREGUÁ',
            'Arroyos y Esteros': 'ARROYOS Y ESTEROS',
            "Ava'í - Cnel. Martinez": "AVA'Í - CNEL. MARTÍNEZ",
            'Ayola': 'AYOLAS',
            'Ayolas': 'AYOLAS',
            'Bañado Sur': 'BAÑADO SUR',
            'Barrio Roberto L. Pettit': 'BARRIO ROBERTO L. PETTIT',
            'Bella vista Norte': 'BELLA VISTA NORTE',
            'Capiata': 'CAPIATÁ',
            'Carlos A. Lopez': 'CARLOS A. LÓPEZ',
            'Carmelo Peralta': 'CARMELO PERALTA',
            'Carayao': 'CARAYAÓ',
            'Cateura': 'CATEURA',
            'Cerrito': 'CERRITO',
            'Chacarita - Parque Caballero': 'CHACARITA - PARQUE CABALLERO',
            'Chore': 'CHORÉ',
            'Ciudad del Este': 'CIUDAD DEL ESTE',
            'Comunidad Indigena Tekoha Sauce': 'COMUNIDAD INDÍGENA TEKOHA SAUCE',
            'Cordillera': 'CORDILLERA',
            'Emboscada': 'EMBOSCADA',
            'Felix Perez Cardozo': 'FÉLIX PÉREZ CARDOZO',
            'Fernando de la Mora': 'FERNANDO DE LA MORA',
            'Filadelfia': 'FILADELFIA',
            'Gral. Diaz': 'GRAL. DÍAZ',
            'Guarambare': 'GUARAMBARÉ',
            'Guayaibi': 'GUAYAIBÍ',
            'Ita': 'ITÁ',
            'Juan Manuel Frutos': 'JUAN MANUEL FRUTOS',
            'La Paz': 'LA PAZ',
            'Lambare': 'LAMBARÉ',
            'Lima': 'LIMA',
            'Limpio': 'LIMPIO',
            'Loma Plata': 'LOMA PLATA',
            'Lombardo': 'LOMBARDO',
            'Mcal. Estigarribia': 'MARISCAL ESTIGARRIBIA',
            'Mbigua': 'MBIGUÁ',
            'Maria Auxiliadora': 'MARÍA AUXILIADORA',
            'Mayor Martinez': 'MAYOR MARTÍNEZ',
            'Nanawa': 'NANAWA',
            'Nueva Colombia': 'NUEVA COLOMBIA',
            'Ñemby': 'ÑEMBY',
            'Paraguari': 'PARAGUARÍ',
            'Pedro P. Peña': 'PEDRO P. PEÑA',
            'Pilar': 'PILAR',
            'Pozo Colorado': 'POZO COLORADO',
            'Pozo Hondo': 'POZO HONDO',
            'Puerto Botanico': 'PUERTO BOTÁNICO',
            'Puerto Casado': 'PUERTO CASADO',
            'Puerto Pinasco': 'PUERTO PINASCO',
            'Pto. Pinasco': 'PUERTO PINASCO',
            'Pto Pinasco': 'PUERTO PINASCO',
            'Repatriacion': 'REPATRIACIÓN',
            'Resquin': 'RESQUÍN',
            'Ricardo Brugada - Chacarita': 'RICARDO BRUGADA - CHACARITA',
            'RI 3 Corrales': 'R.I. TRES CORRALES',
            'Rio Verde': 'RÍO VERDE',
            'Sajonia - Chacarita': 'SAJONIA - CHACARITA',
            'San Estanislao': 'SAN ESTANISLAO',
            'San Juan': 'SAN JUAN',
            'San Lazaro': 'SAN LÁZARO',
            'San Lorenzo': 'SAN LORENZO',
            'San Pablo Cocuere': 'SAN PABLO COCUERÉ',
            'San Pedro del Ycuamandyju': 'SAN PEDRO DEL YCUAMANDIYÚ',
            'San Vicente Pancholo': 'SAN VICENTE PANCHOLO',
            'Santa Ana': 'SANTA ANA',
            'Santa Rosa del Aguaray': 'SANTA ROSA DEL AGUARAY',
            'Sgto. Jose Felix Lopez': 'SGTO. JOSÉ FÉLIX LÓPEZ',
            'Sol Sierra Leona': 'SOL SIERRA LEONA',
            'Tablada Nueva': 'TABLADA NUEVA',
            'Tacuara': 'TACUARÁ',
            'Toro Pampa': 'TORO PAMPA',
            'Tte. Esteban Martinez': 'TTE. ESTEBAN MARTÍNEZ',
            'Tte. Irala Fernandez': 'TTE. IRALA FERNÁNDEZ',
            'Union': 'UNIÓN',
            'Vallemi': 'VALLEMI',
            'Varios Barrios': 'VARIOS BARRIOS',
            'varios': 'VARIOS',
            'Villa Elisa': 'VILLA ELISA',
            'Villa Hayes': 'VILLA HAYES',
            'Villa Oliva': 'VILLA OLIVA',
            'Villalbin': 'VILLALBÍN',
            'Villeta': 'VILLETTA',
            'Yataity del Norte': 'YATAITY DEL NORTE',
            'Yhu': 'YHÚ',
            'Yrybucua': 'YRYBUCUÁ',
            'Yvyrarovana': 'YVYRAROVANA',
            'Yukyty': 'YUKYTY',
            'Zeballos Cue': 'ZEBALLOS CUE',
        }

        # Normalizar estandarizacion_distrito para matching insensible a acentos
        self.estandarizacion_distrito_norm = {}
        for k, v in self.estandarizacion_distrito.items():
            self.estandarizacion_distrito_norm[_norm_str(k)] = v

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
            'Asistencia de la corte': 'C.I.D.H.',
            'ASISTENCIA DE LA CORTE': 'C.I.D.H.',
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
        depto_norm = self._norm_str(depto_limpio)

        # Si el registrador escribió el nombre del distrito en la columna
        # "DEPARTAMENTO", mapearlo al departamento correspondiente (usando mapa normalizado).
        if depto_norm in self.distrito_a_departamento_norm:
            return self.distrito_a_departamento_norm[depto_norm]

        # 1. Búsqueda directa en el diccionario normalizado
        if depto_norm in self.estandarizacion_dept_norm:
            return self.estandarizacion_dept_norm[depto_norm]

        # 2. Si contiene separadores, probar con la primera parte
        for sep in [' - ', ' / ', ', ', ' Y ']:
            if sep in depto_limpio:
                primera_parte = depto_limpio.split(sep)[0].strip()
                if primera_parte in self.estandarizacion_dept:
                    return self.estandarizacion_dept[primera_parte]

        # 3. Coincidencia parcial con nombres válidos
        # 3. Coincidencia parcial con nombres válidos (normalizando también)
        for depto_estandar in self.departamento_orden.keys():
            if self._norm_str(depto_estandar) in depto_norm:
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
            'CIDH': 'C.I.D.H.',
            'Corte': 'C.I.D.H.',
            'CORTE': 'C.I.D.H.',
        }

        for palabra, evento_estandar in palabras_clave.items():
            if palabra in evento_limpio:
                return evento_estandar

        # 3. Sin coincidencias: marcar como sin evento
        return 'SIN EVENTO'


    def inferir_distrito_desde_localidad(self, localidad):
        """Dado el nombre de una localidad limpia, devolver (distrito, departamento)

        Usa el diccionario `self.localidad_map` cuando sea posible.
        Devuelve None si no hay una inferencia clara.
        """
        if pd.isna(localidad) or localidad is None:
            return None

        loc_limpia = self.limpiar_texto(localidad)
        loc_norm = self._norm_str(loc_limpia)
        if loc_norm in self.localidad_map_norm:
            info = self.localidad_map_norm[loc_norm]
            return info.get('distrito'), info.get('departamento')
        return None


    def normalize_locations(self, df):
        """Normaliza y corrige columnas de LOCALIDAD, DISTRITO y DEPARTAMENTO.

        Reglas principales:
        - Si LOCALIDAD está presente y el map conoce su distrito/departamento,
          rellenar DISTRITO/DEPARTAMENTO desde el mapa.
        - Si DEPARTAMENTO contiene un nombre de distrito (según
          `self.distrito_a_departamento`), mover ese valor a DISTRITO y
          reemplazar DEPARTAMENTO por el departamento correspondiente.
        - Si DISTRITO está presente pero DEPARTAMENTO está vacío, inferir
          DEPARTAMENTO mediante `self.distrito_a_departamento`.

        Devuelve el DataFrame modificado (modifica in-place también).
        """
        if df is None or len(df) == 0:
            return df

        # Asegurar columnas mínimas
        for col in ['LOCALIDAD', 'DISTRITO', 'DEPARTAMENTO']:
            if col not in df.columns:
                df[col] = 'SIN ESPECIFICAR'

        cambios = 0

        for idx, row in df.iterrows():
            localidad = row.get('LOCALIDAD', '')
            distrito = row.get('DISTRITO', '')
            departamento = row.get('DEPARTAMENTO', '')

            loc_limpia = self.limpiar_texto(localidad)
            dist_limpia = self.limpiar_texto(distrito)
            depto_limpio = self.limpiar_texto(departamento)

            # Normalizaciones para matching sin acentos
            loc_norm = self._norm_str(loc_limpia)
            dist_norm = self._norm_str(dist_limpia)
            depto_norm = self._norm_str(depto_limpio)

            # 0) Estandarizar DISTRITO si coincide con alguna clave parcial del diccionario
            if dist_limpia not in ['SIN ESPECIFICAR', ''] and hasattr(self, 'estandarizacion_distrito_norm'):
                for k in self.estandarizacion_distrito_norm.keys():
                    if k in dist_norm:
                        df.at[idx, 'DISTRITO'] = self.estandarizacion_distrito_norm[k]
                        dist_limpia = df.at[idx, 'DISTRITO']
                        dist_norm = self._norm_str(dist_limpia)
                        cambios += 1
                        break

            # 1) Si la localidad es conocida, usar el mapa como fuente de verdad
            inferred = None
            if loc_limpia not in ['SIN ESPECIFICAR', '']:
                inferred = self.inferir_distrito_desde_localidad(loc_limpia)

            if inferred:
                inf_dist, inf_depto = inferred
                # Solo escribir si hay diferencia o falta de dato
                if dist_limpia in ['SIN ESPECIFICAR', ''] or dist_limpia != inf_dist:
                    # Usar forma canónica del distrito cuando esté disponible
                    canonic = self.distrito_canonical_map.get(self._norm_str(inf_dist), inf_dist)
                    # Si existe una estandarizacion_distrito que coincida parcialmente, usarla
                    canonic_norm = self._norm_str(canonic)
                    if hasattr(self, 'estandarizacion_distrito_norm'):
                        for k in self.estandarizacion_distrito_norm.keys():
                            if k in canonic_norm:
                                canonic = self.estandarizacion_distrito_norm[k]
                                break
                    df.at[idx, 'DISTRITO'] = canonic
                    cambios += 1
                if depto_limpio in ['SIN ESPECIFICAR', ''] or depto_limpio != inf_depto:
                    df.at[idx, 'DEPARTAMENTO'] = inf_depto
                    cambios += 1
                continue

            # 2) Si en DEPARTAMENTO hay un distrito (registrador se equivocó), moverlo
            if depto_norm in self.distrito_a_departamento_norm:
                mapped_depto = self.distrito_a_departamento_norm[depto_norm]
                # Mover el valor de departamento al campo DISTRITO
                # Establecer forma canónica del distrito si está disponible
                canonic_depto_as_distr = self.distrito_canonical_map.get(depto_norm, depto_limpio)
                df.at[idx, 'DISTRITO'] = canonic_depto_as_distr
                df.at[idx, 'DEPARTAMENTO'] = mapped_depto
                cambios += 1
                continue

            # 3) Si DISTRITO está presente pero DEPARTAMENTO vacío, completar
            if dist_limpia not in ['SIN ESPECIFICAR', ''] and (depto_limpio in ['SIN ESPECIFICAR', '']):
                if dist_norm in self.distrito_a_departamento_norm:
                    df.at[idx, 'DEPARTAMENTO'] = self.distrito_a_departamento_norm[dist_norm]
                    # Asegurar que DISTRITO tenga la forma canónica
                    df.at[idx, 'DISTRITO'] = self.distrito_canonical_map.get(dist_norm, dist_limpia)
                    cambios += 1

        if cambios > 0:
            print(f"  Normalización de ubicaciones aplicada. Cambios realizados: {cambios}")
        else:
            print("  Normalización de ubicaciones: no se detectaron cambios relevantes.")

        return df


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

        # Conteo inicial para diagnóstico
        registros_iniciales = len(df)
        print(f"  Registros iniciales: {registros_iniciales}")

        # Normalizar nombres de columnas a MAYÚSCULAS con guiones bajos
        df.columns = [col.upper().replace(' ', '_') for col in df.columns]

        # 1. Normalizar ubicaciones usando los diccionarios (LOCALIDAD/DISTRITO/DEPARTAMENTO)
        df = self.normalize_locations(df)

        # 2. Estandarizar departamentos (ahora que los valores están en las columnas correctas)
        if 'DEPARTAMENTO' in df.columns:
            df['DEPARTAMENTO'] = df['DEPARTAMENTO'].apply(self.estandarizar_departamento_robusto)

        # 2. Estandarizar eventos (antes de la inferencia)
        if 'EVENTO' in df.columns:
            df['EVENTO'] = df['EVENTO'].apply(self.estandarizar_evento_robusto)
            # Mostrar distribución inicial de eventos para diagnóstico
            try:
                print("  Distribución EVENTO (pre-inferencia):")
                print(df['EVENTO'].value_counts(dropna=False).to_string())
            except Exception:
                pass

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

        print(f"  Eventos inferidos/ajustados: {eventos_inferidos}")
        try:
            print("  Distribución EVENTO (post-inferencia):")
            print(df['EVENTO'].value_counts(dropna=False).to_string())
        except Exception:
            pass
        
        # Eliminar columnas temporales
        cols_to_drop = [f'{col}_TEMP' for col in insumos_cols_map.keys() if f'{col}_TEMP' in df.columns]
        df = df.drop(columns=cols_to_drop, errors='ignore')

        # 4. Eliminar registros marcados (preposicionamiento) y aquellos sin insumos
        registros_antes = len(df)
        print(f"  Registros antes de eliminación: {registros_antes}")

        # Realizar la limpieza de números en las columnas de insumos para el cálculo total
        for col in insumos_cols_map.keys():
            df[col] = df.get(col, pd.Series([0] * len(df), index=df.index)).apply(self.limpiar_numero)

        # Calcular el total de insumos
        insumo_cols = list(insumos_cols_map.keys())
        df['TOTAL_INSUMOS'] = df[insumo_cols].sum(axis=1)

        # 4a. Eliminar ELIMINAR_REGISTRO (Preposicionamiento)
        registros_eliminados_prepos = int((df['EVENTO'] == 'ELIMINAR_REGISTRO').sum())
        df_limpio = df[df['EVENTO'] != 'ELIMINAR_REGISTRO'].copy()
        print(f"  Registros marcados ELIMINAR_REGISTRO: {registros_eliminados_prepos}")

        # 4b. Eliminar registros sin insumos
        registros_sin_insumos = int((df_limpio['TOTAL_INSUMOS'] <= 0).sum()) if 'TOTAL_INSUMOS' in df_limpio.columns else 0
        df_limpio = df_limpio[df_limpio['TOTAL_INSUMOS'] > 0]
        registros_eliminados_cero = registros_sin_insumos
        print(f"  Registros sin insumos (TOTAL_INSUMOS<=0): {registros_sin_insumos}")

        df = df_limpio.drop(columns=['TOTAL_INSUMOS'], errors='ignore')

        print(f"  Registros eliminados (Preposicionamiento): {registros_eliminados_prepos:,}")
        print(f"  Registros eliminados (Sin insumos): {registros_eliminados_cero:,}")
        print(f"  Registros restantes: {len(df):,}")

        # 5. Generar columnas derivadas (AÑO, MES, ORDEN_DEPARTAMENTO, ...)
        df = self.feature_engineering_basico(df)

        # 6. Asegurar esquema y tipos para la carga en el DW
        df = self.estandarizacion_final_columnas(df)

        return df

    # ... (feature_engineering_basico, estandarizacion_final_columnas y verificacion_final se mantienen iguales)
    def feature_engineering_basico(self, df):
        """Feature engineering básico (Alineado con tu archivo original)"""
        # print("⚙️ Aplicando feature engineering...")

        # Buscar la columna de fecha (la primera que contenga 'FECHA')
        fecha_cols = [col for col in df.columns if 'FECHA' in col.upper()]
        if fecha_cols:
            col_fecha = fecha_cols[0]

            # Intento 1: parseo directo con inferencia y dayfirst (común en latam)
            df[col_fecha] = pd.to_datetime(df[col_fecha], errors='coerce', dayfirst=True, infer_datetime_format=True)

            # Si quedan valores inválidos, intentamos heurísticas adicionales
            n_invalid_fecha = int(df[col_fecha].isna().sum())
            if n_invalid_fecha > 0:
                print(f"  Nota: {n_invalid_fecha} filas inicialmente no parsearon como fecha. Aplicando heurísticas de recuperación...")

                # Heurística A: detectar números tipo Excel serial (valores enteros grandes)
                def try_excel_serial(val):
                    try:
                        if pd.isna(val):
                            return None
                        # Si es numérico y razonable como serial de Excel
                        if isinstance(val, (int, float)) and val > 1000:
                            # Excel's epoch (for Windows) -> 1899-12-30
                            return (pd.to_datetime('1899-12-30') + pd.to_timedelta(int(val), unit='D'))
                        # También aceptar strings que sean dígitos
                        if isinstance(val, str) and re.fullmatch(r"\d+", val.strip()):
                            iv = int(val.strip())
                            if iv > 1000:
                                return (pd.to_datetime('1899-12-30') + pd.to_timedelta(iv, unit='D'))
                    except Exception:
                        return None
                    return None

                # Aplicar heurística de serials donde la fecha sea NaT
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

                # Heurística B: intentar construir fecha desde columnas AÑO/ANO/YEAR y MES/MONTH
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

                # Actualizar conteo de inválidos tras heurísticas
                n_invalid_fecha_after = int(df[col_fecha].isna().sum())
                n_recovered_total = n_invalid_fecha - n_invalid_fecha_after
                if n_recovered_total > 0:
                    print(f"  Heurísticas recuperaron {n_recovered_total} fechas. {n_invalid_fecha_after} siguen inválidas.")

                # Si aún quedan muchas fechas inválidas, mostrar ejemplos para diagnóstico
                if n_invalid_fecha_after > 0:
                    sample_invalid = df[df[col_fecha].isna()].head(10)
                    print("  Ejemplos de valores de FECHA no parseados (primeros 10):")
                    for i, r in sample_invalid.iterrows():
                        print(f"    idx={i} valor_original={repr(r.get(col_fecha))}")

            # Después de todas las estrategias, eliminar filas sin fecha válida
            final_invalid = int(df[col_fecha].isna().sum())
            if final_invalid > 0:
                print(f"  Advertencia: {final_invalid} filas siguen sin FECHA válida y serán descartadas antes de la carga.")
                df = df[df[col_fecha].notna()].copy()

            # Generar columnas AÑO y MES desde la fecha ya saneada
            df['AÑO'] = df[col_fecha].dt.year
            df['MES'] = df[col_fecha].dt.month

            # Detectar fechas con años no realistas (por ejemplo 1900) y descartarlas
            mask_invalid_year = df['AÑO'] <= 1900
            n_invalid_years = int(mask_invalid_year.sum())
            if n_invalid_years > 0:
                print(f"  Advertencia: {n_invalid_years} filas tienen AÑO <= 1900 y serán descartadas (fechas inválidas).")
                df = df[~mask_invalid_year].copy()

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
                print(f"  - {evento}: {count}")

        return df # Devuelve el DF para encadenamiento si es necesario