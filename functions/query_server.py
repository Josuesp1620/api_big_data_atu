from pypika import Query, Table, functions as fn, Order

import requests
# URL de ejemplo
url = 'http://200.121.128.47:3072/api'

def get_data_api(params):
    # Realizar la solicitud POST
    response = requests.get(f'{url}/sql', params=params)
    # Verificar el estado de la respuesta
    if response.status_code == 200:
        # La solicitud fue exitosa
        return response.json() # Convertir la respuesta a JSON si es aplicable
    else:
        # La solicitud no fue exitosa, imprimir el código de estado
        print("Error al hacer la solicitud. Código de estado:", response.status_code)

def get_data_centroid_api(params={'format':'json', 'limit':10}, body=None, tag_name=None, name_name=None):
    # Realizar la solicitud POST
    response = requests.post(f'{url}/v1/{tag_name}/{name_name}', params=params, json=body)
    # Verificar el estado de la respuesta
    if response.status_code == 200:
        # La solicitud fue exitosa
        return response.json() # Convertir la respuesta a JSON si es aplicable
    else:
        # La solicitud no fue exitosa, imprimir el código de estado
        print("Error al hacer la solicitud. Código de estado:", response.status_code)


def create_query_sum_all_viajes(query_target, table_name):
    table = Table(table_name)

    filtered_query = {key: value for key, value in query_target.items() if len(value) != 0}

    # SELECT
    query = Query.from_(table).select(fn.Sum(table.viajes, "sum_viajes_all"))

    # WHERE dinámico
    for key, value in filtered_query.items():
        if isinstance(value, list):
            query = query.where(getattr(table, key).isin(value))
        else:
            query = query.where(getattr(table, key) == value)
    return str(query)


def create_query_get_data_for_arc_layer(query_target, table_name, limit=5, order_by="top_min"):
    table = Table(table_name)

    filtered_query = {key: value for key, value in query_target.items() if len(value) != 0}

    field = [key for key, value in filtered_query.items() if key.startswith("taz_")]
    query = Query.from_(table).select(field[0].replace("_o", "_d"), fn.Sum(table.viajes).as_("suma_viajes")).limit(limit)

    # WHERE dinámico
    for key, value in filtered_query.items():
        if isinstance(value, list):
            query = query.where(getattr(table, key).isin(value))
        else:
            query = query.where(getattr(table, key) == value)

    # ORDER BY
    query = query.groupby(field[0].replace("_o", "_d"))
    if order_by == "top_max":
        query = query.orderby("suma_viajes", order=Order.desc)
    else:
        query = query.orderby("suma_viajes", order=Order.asc)
    return str(query)

def create_query_get_data_for_export_excel(query_target, table_name):
    table = Table(table_name)

    filtered_query = {key: value for key, value in query_target.items() if len(value) != 0}

    query = Query.from_(table).select("*")

    # WHERE dinámico
    for key, value in filtered_query.items():
        if isinstance(value, list):
            query = query.where(getattr(table, key).isin(value))
        else:
            query = query.where(getattr(table, key) == value)
    sql_query = str(query).replace('\"', '')
    return f"{url}/sql?sql={sql_query}&format=xlsx"

def query_get_data_calculate_dashboard(query_target, table_name, f_calculate):
    table = Table(table_name)

    filtered_query = {key: value for key, value in query_target.items() if len(value) != 0}

    query = Query.from_(table).select(f_calculate, fn.Function("ROUND",fn.Sum(table.viajes), 2).as_("suma_viajes"))

    # WHERE dinámico
    for key, value in filtered_query.items():
        if isinstance(value, list):
            query = query.where(getattr(table, key).isin(value))
        else:
            query = query.where(getattr(table, key) == value)

    # ORDER BY
    query = query.groupby(f_calculate)
    return str(query)
