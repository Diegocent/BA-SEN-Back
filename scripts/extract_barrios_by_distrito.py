"""Extrae barrios/localidades agrupados por distrito desde un GeoJSON de barrios.

Salida: JSON con estructura {"DISTRITO_NORMALIZADO": ["Barrio 1", "Barrio 2", ...], ...}

Uso: ejecutar desde PowerShell:
python .\scripts\extract_barrios_by_distrito.py "..\datos_cartografia_ine\territorio\TOTAL PAIS\Barrios Localidades Paraguay_2022.geojson" output_barrios.json

Si no se pasa output, imprimirá el JSON por stdout.
"""
import json
import sys
import os
import unicodedata
import re


def _norm_str(s: str) -> str:
    if s is None:
        return ''
    s2 = str(s).upper().strip()
    s2 = unicodedata.normalize('NFKD', s2)
    s2 = ''.join(ch for ch in s2 if not unicodedata.combining(ch))
    s2 = re.sub(r"\s+", " ", s2)
    return s2


def extract_barrios_by_distrito(geojson_path: str):
    if not os.path.exists(geojson_path):
        raise FileNotFoundError(geojson_path)

    with open(geojson_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    features = data.get('features', [])

    # Posibles keys que suelen contener el nombre del distrito y del barrio/localidad
    distrito_keys = ['DIST_DESC_', 'DIST_DESC', 'DISTRITO', 'DIST', 'DPTO_DESC', 'DPTO']
    # Priorizar claves encontradas en el GeoJSON de Barrios: BARLO_DESC contiene el nombre legible
    barrio_keys = ['BARLO_DESC', 'BAR_LOC', 'CLAVE_BAR', 'BARRIO', 'LOCALIDAD', 'NOMBRE', 'NAME', 'NOM_BARRIO', 'NOM_LOCALIDAD', 'NOMBRE_BARRIO', 'BARRIO_NOMBRE']

    # Resultado: mapa distrito_normalizado -> set de barrios originales
    resultado = {}

    for feat in features:
        props = feat.get('properties', {}) or {}

        # Buscar campo de distrito
        distrito_raw = None
        for k in distrito_keys:
            if k in props and props[k] not in [None, '']:
                distrito_raw = props[k]
                break

        # Buscar campo de barrio/localidad (preferir BARLO_DESC en estos datos)
        barrio_raw = None
        for k in barrio_keys:
            if k in props and props[k] not in [None, '']:
                barrio_raw = props[k]
                break

        # Si no encontramos barrio explícito, intentar con otras propiedades comunes
        if barrio_raw is None:
            # buscar cualquier propiedad que contenga substring 'BARR' o 'LOCAL' en el nombre
            for k, v in props.items():
                if v and isinstance(v, str):
                    kk = k.upper()
                    if 'BARR' in kk or 'LOCAL' in kk or 'LOCA' in kk or 'BARRIO' in kk:
                        barrio_raw = v
                        break

        # Normalizar clave de distrito
        distrito_norm = _norm_str(distrito_raw) if distrito_raw else 'SIN DISTRITO'

        # Normalizar barrio: mantener la forma original (con acentos) pero limpiar espacios
        barrio_val = None
        if barrio_raw:
            barrio_val = str(barrio_raw).strip()
            if barrio_val == '':
                barrio_val = None
        else:
            # intentar extraer de otras propiedades si no se encontró en las claves preferidas
            for alt in ['NOMBRE', 'NAME', 'NOM', 'LOC_NAME', 'BARRIO', 'LOCALIDAD']:
                nombre = props.get(alt)
                if nombre and str(nombre).strip() != '':
                    barrio_val = str(nombre).strip()
                    break

        if barrio_val is None or barrio_val == '':
            # usar indicador genérico
            barrio_val = 'SIN_NOMBRE'

        # Añadir al resultado
        resultado.setdefault(distrito_norm, set()).add(barrio_val)

    # Convertir sets a listas ordenadas
    resultado_list = {k: sorted(list(v)) for k, v in resultado.items()}
    return resultado_list


def main():
    if len(sys.argv) < 2:
        print('Uso: python extract_barrios_by_distrito.py <path_geojson> [output.json]')
        sys.exit(1)

    geojson_path = sys.argv[1]
    output = sys.argv[2] if len(sys.argv) > 2 else None

    out = extract_barrios_by_distrito(geojson_path)

    if output:
        with open(output, 'w', encoding='utf-8') as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f'Escrito: {output} (distritos: {len(out)})')
    else:
        print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
