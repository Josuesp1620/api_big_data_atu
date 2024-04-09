from pypika import Query, Table, functions as fn, Order, enums

import requests
# URL de ejemplo
url = 'http://200.121.128.47:3072/api'

def get_data_api(params):
    user = params['credentials']['user']
    password = params['credentials']['password']
    del params['credentials']
    # Realizar la solicitud POST
    response = requests.get(f'{url}/sql', params=params, auth=(user, password))
    # Verificar el estado de la respuesta
    if response.status_code == 200:
        # La solicitud fue exitosa
        return response.json() # Convertir la respuesta a JSON si es aplicable
    else:
        # La solicitud no fue exitosa, imprimir el código de estado
        print('Error al hacer la solicitud. Código de estado:', response.status_code)

def get_data_centroid_api(params={'format':'json', 'limit':10}, body=None, tag_name=None, name_name=None, credentials=None):
    # Realizar la solicitud POST
    response = requests.post(f'{url}/v1/{tag_name}/{name_name}', params=params, json=body, auth=(credentials["user"], credentials["password"]))
    # Verificar el estado de la respuesta
    if response.status_code == 200:
        # La solicitud fue exitosa
        return response.json() # Convertir la respuesta a JSON si es aplicable
    else:
        # La solicitud no fue exitosa, imprimir el código de estado
        print('Error al hacer la solicitud. Código de estado:', response.status_code)


def create_query_sum_all_viajes(query_target, table_name):
    table = Table(table_name)

    filtered_query = {key: value for key, value in query_target.items() if len(value) != 0}

    # SELECT
    query = Query.from_(table).select(fn.Cast(fn.Function('ROUND',fn.Sum(table.viajes), -1), enums.SqlTypes.INTEGER).as_('sum_viajes_all'))

    # WHERE dinámico
    for key, value in filtered_query.items():
        if isinstance(value, list):
            query = query.where(getattr(table, key).isin(value))
        else:
            query = query.where(getattr(table, key) == value)
    return str(query)


def create_query_get_data_for_arc_layer(query_target, table_name, field, limit=5, order_by='top_min'):
    table = Table(table_name)

    filtered_query = {key: value for key, value in query_target.items() if len(value) != 0}

    if(field.endswith("_d")):
        if('dist' in field):
            field = field.replace('_dist_d', '_dist_o')
        else:
            field = field.replace('_d', '_o')
    else:
        if('dist' in field):
            field = field.replace('_dist_o', '_dist_d')
        else:
            field = field.replace('_o', '_d')
    query = Query.from_(table).select(field, fn.Cast(fn.Function('ROUND',fn.Sum(table.viajes), -1), enums.SqlTypes.INTEGER).as_('suma_viajes')).limit(limit)

    # WHERE dinámico
    for key, value in filtered_query.items():
        if isinstance(value, list):
            query = query.where(getattr(table, key).isin(value))
        else:
            query = query.where(getattr(table, key) == value)

    # ORDER BY
    query = query.groupby(field)
    if order_by == 'top_max':
        query = query.orderby('suma_viajes', order=Order.desc)
    else:
        query = query.orderby('suma_viajes', order=Order.asc)
    return str(query)

def create_query_get_data_for_export_excel(query_target, table_name):
    table = Table(table_name)

    filtered_query = {key: value for key, value in query_target.items() if len(value) != 0}

    query = Query.from_(table).select('*')

    # WHERE dinámico
    for key, value in filtered_query.items():
        if isinstance(value, list):
            query = query.where(getattr(table, key).isin(value))
        else:
            query = query.where(getattr(table, key) == value)
    sql_query = str(query).replace('\'', '')
    return f'{sql_query}'

def download_files(url_download):
    # Realizar la solicitud GET para obtener el archivo
    respuesta = requests.get(url_download, stream=True)  # Usar stream=True para descargar por chunks

    # Verificar si la solicitud fue exitosa (código 200)
    if respuesta.status_code == 200:
        # Guardar el contenido de la respuesta en un archivo local por chunks
        with open('archivo_descargado.xlsx', 'wb') as archivo:
            for chunk in respuesta.iter_content(chunk_size=1024):  # Descargar chunks de 1024 bytes
                if chunk:  # Verificar que el chunk tenga contenido
                    archivo.write(chunk)
        print('Archivo descargado correctamente.')
    else:
        print('Error al descargar el archivo:', respuesta.status_code)

def query_get_data_calculate_dashboard(query_target, table_name, field, f_calculate):
    table = Table(table_name)

    filtered_query = {key: value for key, value in query_target.items() if len(value) != 0}

    
    if(field.endswith("_d")):
        if('dist' in field):
            field = field.replace('_dist_d', '_dist_o')
        else:
            field = field.replace('_d', '_o')
    else:
        if('dist' in field):
            field = field.replace('_dist_o', '_dist_d')
        else:
            field = field.replace('_o', '_d')

    query = Query.from_(table).select(f_calculate, table[field].as_("taz"), fn.Cast(fn.Function('ROUND',fn.Sum(table.viajes), -1), enums.SqlTypes.INTEGER).as_('suma_viajes'))

    # WHERE dinámico
    for key, value in filtered_query.items():
        if isinstance(value, list):
            query = query.where(getattr(table, key).isin(value))
        else:
            query = query.where(getattr(table, key) == value)

    # ORDER BY
    query = query.groupby(f_calculate)
    query = query.groupby(field)

    return str(query)
