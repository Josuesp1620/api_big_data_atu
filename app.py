from flask import Flask, jsonify, request
from flask_cors import CORS
import numpy as np
app = Flask(__name__)
CORS(app)

from functions.query_server import (
    create_query_get_data_for_arc_layer, 
    get_data_api, 
    create_query_sum_all_viajes, 
    get_data_centroid_api,
    query_get_data_calculate_dashboard,
    create_query_get_data_for_export_excel,
    download_files
)
import concurrent.futures

from geojson import Feature
from shapely.geometry import Point
from shapely.wkt import loads

def Random_Points_in_Polygon(polygon, number):
    points = []
    minx, miny, maxx, maxy = polygon.bounds
    while len(points) < number:
        pnt = Point(np.random.uniform(minx, maxx), np.random.uniform(miny, maxy))
        if polygon.contains(pnt):
            points.append(pnt)
    return points

@app.route('/filter_data', methods=['POST'])
def filter_data():
    query_target = request.json
    querty_type = query_target["type"]
    querty_limit = int(query_target["limit"])
    querty_order_by = query_target["order_by"]
    del query_target["type"]
    del query_target["limit"]
    del query_target["order_by"]
    max_suma_viajes = 0

    # Función para realizar una consulta
    def execute_query(params):
        # Aquí deberías tener la lógica para ejecutar tu consulta utilizando la API
        # En este ejemplo, simplemente se devuelve un mensaje ficticio
        return get_data_api(params=params)

    # Función para crear las consultas  
    def create_queries():
        query_destination = create_query_get_data_for_arc_layer(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', limit=querty_limit, order_by=querty_order_by)
        query_sum_all = create_query_sum_all_viajes(query_target=query_target, table_name='source_target_parquet_data_mayo_2019')
        # query_all = create_query_get(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', columns="all")
        return [{"sql": query_destination}, {"sql": query_sum_all}]

    # Obtiene las consultas a ejecutar
    queries = create_queries()

    # Ejecuta ambas consultas en paralelo
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Ejecuta las consultas y guarda los resultados en una lista
        results = list(executor.map(execute_query, queries))

    field = [key for key, value in query_target.items() if len(value) != 0 and key.startswith("taz_") and key.endswith("_o")][0]
    
    # Función para obtener los datos del API de centroides
    def get_centroid_data(item, field):
        nonlocal max_suma_viajes  # Agregar esta línea
        target = get_data_centroid_api(body={"taz":item[field.replace('_o', '_d')]}, tag_name="data_centroid", name_name=querty_type)[0]        
        if item[field.replace('_o', '_d')] == source_result["taz"]:
            points = Random_Points_in_Polygon(loads(target["geometry"]), 1)        
            target['lat'] = points[0].y
            target['lon'] = points[0].x
        max_suma_viajes += item["suma_viajes"]
        target['suma_viajes'] = "{:,.2f}".format(item["suma_viajes"])
        del target['geometry']
        return target

    # Obtener los datos fuente
    source_result = get_data_centroid_api(body={"taz":query_target[field][0]}, tag_name="data_centroid", name_name=querty_type)[0]
    del source_result['geometry']

    # Obtener los resultados en paralelo
    target_final = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Mapear las llamadas a la función get_centroid_data con los datos de entrada
        future_to_item = {executor.submit(get_centroid_data, item, field): item for item in results[0]}
        for future in concurrent.futures.as_completed(future_to_item):
            try:
                result = future.result()
                target_final.append(result)
            except Exception as exc:
                print(f'Error en la llamada: {exc}')

    # Crear una lista de features
    source_result["centroid"] = source_result['lon'], source_result['lat']
    source_result["type"] = "source"
    source_result["max_suma_viajes"] = max_suma_viajes
    source_result = Feature(geometry=Point((source_result['lon'], source_result['lat'])), properties=source_result)
    
    # Crear una lista de features
    features_taget = []
    for feature in target_final:
        # Crear un punto GeoJSON para cada lugar
        point = Point((feature['lon'], feature['lat']))
        # Crear una característica GeoJSON para el punto
        feature["centroid"] = feature['lon'], feature['lat']
        feature["type"] = "target"
        feature = Feature(geometry=point, properties=feature)
        features_taget.append(feature)

    # download_files(create_query_get_data_for_export_excel(query_target=query_target, table_name='source_target_parquet_data_mayo_2019'))
    response = {
        'status': 'success',
        'data': {
            'source': source_result,
            'target': features_taget,
            'url_xlsx' : create_query_get_data_for_export_excel(query_target=query_target, table_name='source_target_parquet_data_mayo_2019')
        }
    }

    return jsonify({
                "data" : response
            }), 200

@app.route('/data_dash_board', methods=['POST'])
def data_dash_board():
    query_target = request.json
    del query_target["type"]
    del query_target["limit"]
    del query_target["order_by"]

    def execute_query(params):
        return get_data_api(params=params)

    def create_queries():
        query_horario = query_get_data_calculate_dashboard(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', f_calculate="horario")
        query_edad = query_get_data_calculate_dashboard(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', f_calculate="edad")
        query_nse = query_get_data_calculate_dashboard(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', f_calculate="nse")
        query_tipo_dia = query_get_data_calculate_dashboard(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', f_calculate="tipo_dia")
        query_motivo = query_get_data_calculate_dashboard(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', f_calculate="motivo")
        query_genero = query_get_data_calculate_dashboard(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', f_calculate="genero")

        return [{"sql": query_horario}, {"sql": query_edad}, {"sql": query_nse}, {"sql": query_tipo_dia}, {"sql": query_motivo}, {"sql": query_genero}]

    queries = create_queries()

    # Ejecuta las consultas en paralelo
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(execute_query, queries))

    formatted_data = {}

    for group in results:
        key = list(group[0].keys())[0]
        formatted_data[key] = group

    response = {
        'status': 'success',
        'data': formatted_data
    }

    return jsonify({
                "data" : response
            }), 200

if __name__ == '__main__':
    app.run(debug=True, port=3071, host='0.0.0.0')
