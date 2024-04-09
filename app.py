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
    create_query_get_data_for_export_excel
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

def get_data_body(request):
    query_target = request.json
    query_type = query_target["type"]
    query_limit = int(query_target["limit"])
    query_order_by = query_target["order_by"]
    del query_target["type"]
    del query_target["limit"]
    del query_target["order_by"]
    return {    
        "query_target": query_target,
        "query_type": query_type,
        "query_limit": query_limit,
        "query_order_by": query_order_by
    }

def execute_queries_initial(query_target, query_limit, query_order_by, field, credentials):
    # Función para realizar una consulta
    def execute_query(params):
        # Aquí deberías tener la lógica para ejecutar tu consulta utilizando la API
        # En este ejemplo, simplemente se devuelve un mensaje ficticio
        return get_data_api(params=params)

    # Función para crear las consultas  
    def create_queries():
        query_destination = create_query_get_data_for_arc_layer(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', limit=query_limit, order_by=query_order_by, field=field)
        query_sum_all = create_query_sum_all_viajes(query_target=query_target, table_name='source_target_parquet_data_mayo_2019')
        # query_all = create_query_get(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', columns="all")
        return [{"sql": query_destination, "credentials": credentials}, {"sql": query_sum_all, "credentials": credentials}]
        # Obtiene las consultas a ejecutar
    queries = create_queries()

    # Ejecuta ambas consultas en paralelo
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Ejecuta las consultas y guarda los resultados en una lista
        results = list(executor.map(execute_query, queries))
    return results

def get_one_data_centroid(query_target, query_type, field, credentials):
    _type = "source"

    if(field.endswith("_d")):
        _type = "target"

    # Obtener los datos fuente
    result = get_data_centroid_api(body={"taz":query_target[field][0]}, tag_name="data_centroid", name_name=query_type, credentials=credentials)[0]
    del result['geometry']
    # Crear una lista de features
    result["centroid"] = result['lon'], result['lat']
    result["type"] = _type
    result = Feature(geometry=Point((result['lon'], result['lat'])), properties=result)
    return result

def get_multiple_data_centroid(one_data_centroid, query_type, field, results_queries_initial, credentials):
    max_suma_viajes = 0
    _type = "target"
    if(field.endswith("_d")):
        _type = "source"
        if('dist' in field):
            field = field.replace('_dist_d', '_dist_o')
        else:
            field = field.replace('_d', '_o')
    else:
        if('dist' in field):
            field = field.replace('_dist_o', '_dist_d')
        else:
            field = field.replace('_o', '_d')
    # Función para obtener los datos del API de centroides
    def get_centroid_data(item, field):
        nonlocal max_suma_viajes
        target = get_data_centroid_api(body={"taz":item[field]}, tag_name="data_centroid", name_name=query_type, credentials=credentials)[0]                
        if item[field] == one_data_centroid["properties"]["taz"]:
            points = Random_Points_in_Polygon(loads(target["geometry"]), 1)        
            target['lat'] = points[0].y
            target['lon'] = points[0].x   
        max_suma_viajes += item["suma_viajes"]     
        target['suma_viajes'] = item["suma_viajes"]
        del target['geometry']
        return target

    # Obtener los resultados en paralelo
    target_final = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Mapear las llamadas a la función get_centroid_data con los datos de entrada
        future_to_item = {executor.submit(get_centroid_data, item, field): item for item in results_queries_initial[0]}
        for future in concurrent.futures.as_completed(future_to_item):
            try:
                result = future.result()
                target_final.append(result)
            except Exception as exc:
                print(f'Error en la llamada: {exc}')

    # Crear una lista de features
    features_taget = []
    for feature in target_final:
        # Crear un punto GeoJSON para cada lugar
        point = Point((feature['lon'], feature['lat']))
        # Crear una característica GeoJSON para el punto
        feature["centroid"] = feature['lon'], feature['lat']
        feature["type"] = _type
        feature = Feature(geometry=point, properties=feature)
        features_taget.append(feature)
    return {
        "suma_viajes": max_suma_viajes,
        "features_taget": features_taget
    }

def getUserCredentials():
    # Obtener las credenciales de la URL (si existen)
    username = request.authorization.username if request.authorization else None
    password = request.authorization.password if request.authorization else None
    return {
        "user": username,
        "password": password,
    }

@app.route('/filter_data', methods=['POST'])
def filter_data():
    data_body = get_data_body(request)
    credentials = getUserCredentials()
    field = [key for key, value in data_body["query_target"].items() if len(value) != 0 and key.startswith("taz_")][0]
    results_queries_initial = execute_queries_initial(query_limit=data_body["query_limit"], query_target=data_body["query_target"], query_order_by=data_body["query_order_by"], field=field, credentials=credentials)    
    one_data_centroid = get_one_data_centroid(query_target=data_body["query_target"], query_type=data_body["query_type"], field=field, credentials=credentials)
    multiple_data_centroid = get_multiple_data_centroid(one_data_centroid=one_data_centroid, query_type=data_body["query_type"], field=field, results_queries_initial=results_queries_initial, credentials=credentials)

    response = {
        'status': 'success',
        'data': {
            'source': one_data_centroid,
            'target': multiple_data_centroid["features_taget"],
            'url_xlsx' : create_query_get_data_for_export_excel(query_target=data_body["query_target"], table_name='source_target_parquet_data_mayo_2019'),
            'sum_all_viajes': results_queries_initial[1][0]['sum_viajes_all'],
            "suma_viajes": multiple_data_centroid["suma_viajes"]
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
    credentials = getUserCredentials()
    field = [key for key, value in query_target.items() if len(value) != 0 and key.startswith("taz_")][0]

    def execute_query(params):
        return get_data_api(params=params)

    def create_queries():
        query_horario = query_get_data_calculate_dashboard(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', field=field, f_calculate="horario")
        query_edad = query_get_data_calculate_dashboard(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', field=field, f_calculate="edad")
        query_nse = query_get_data_calculate_dashboard(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', field=field, f_calculate="nse")
        query_tipo_dia = query_get_data_calculate_dashboard(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', field=field, f_calculate="tipo_dia")
        query_motivo = query_get_data_calculate_dashboard(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', field=field, f_calculate="motivo")
        query_genero = query_get_data_calculate_dashboard(query_target=query_target, table_name='source_target_parquet_data_mayo_2019', field=field, f_calculate="genero")

        return [{"sql": query_horario, "credentials": credentials}, {"sql": query_edad, "credentials": credentials}, {"sql": query_nse, "credentials": credentials}, {"sql": query_tipo_dia, "credentials": credentials}, {"sql": query_motivo, "credentials": credentials}, {"sql": query_genero, "credentials": credentials}]

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
