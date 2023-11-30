from fastapi import FastAPI
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
import sqlalchemy
from sqlalchemy import create_engine


app = FastAPI()


# Configura la conexión a tu base de datos MySQL
db_user = "EdwinGerman"
db_pass = "Tijuanamp5"
db_host = "agrimarketsql.mysql.database.azure.com"
db_name = "agrimarket"

# Cadena de conexión para MySQL
db_url = f"mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}/{db_name}"

# Crea el motor de la base de datos
engine = create_engine(db_url)

def fetch_orders_from_db():
    query = "SELECT * FROM orders"  
    with engine.connect() as connection:
        df = pd.read_sql(query, connection, dtype={
            'id': str,
            'client_id': str,
            'farmer_id': str,
            'product_id': str,
            'quantity': float,  
            'unit_of_measurement_id': str,
            'total': float,  
            'status': str,
            'active': int,  
            'created_at': str,
            'updated_at': str
        })

    return df


def generate_sample_data():
    start_date = datetime.now() - timedelta(days=360)
    end_date = datetime.now()

    dates = pd.date_range(start=start_date, end=end_date, freq='D')

    client_ids = [
    '9abb9f74-754f-49c4-8f50-8f139581ac4b',
    '9abba000-9fb6-474d-8327-8d3957b5c6ac',
    '9abba0aa-978e-40b2-9797-81660e655092',
    '9abba0dc-51e9-4a3b-8dec-1ca3b58b7eb0'
    ]

    # Definir las relaciones entre agricultores y productos
    farmers_and_products = {
        '9ab1106d-6913-4c59-9e8d-707d2ec2822b': ['9ab610b4-6982-492a-b612-e360d3f49a7c'],
        '9ab5eed9-8d0a-4dfd-a70e-c63c404a335f': ['9ab612b4-b6e9-4196-a32b-21bae17324d7'],
        '9ab1106d-6913-4c59-9e8d-707d2ec2822b': ['9ab57076-47e9-48aa-a77e-327cc0c8e450'],
        '9ab1106d-6913-4c59-9e8d-707d2ec2822b': ['9ab5773d-73b3-49fa-ac44-0cb7b924dc6f']
    }

    # Generar los pedidos
    data = {
        'id': [],
        'client_id': [],
        'farmer_id': [],
        'product_id': [],
        'quantity': [],
        'unit_of_measurement_id': [],
        'total': [],
        'status': [],
        'active': [],
        'created_at': [],
        'updated_at': []
    }

    # Generar órdenes para cada día
    for date in dates:
        num_orders = np.random.poisson(20)  # Generar un número de órdenes para este día
        for _ in range(num_orders):
            farmer = np.random.choice(list(farmers_and_products.keys()))
            product = np.random.choice(farmers_and_products[farmer])

            data['id'].append(len(data['id']) + 1)
            data['client_id'].append(np.random.choice(client_ids))
            data['farmer_id'].append(farmer)
            data['product_id'].append(product)
            data['quantity'].append(np.random.randint(1, 10))
            data['unit_of_measurement_id'].append("9a91e357-d777-4a26-86f5-6c33fdd7ee8f")
            data['total'].append(np.random.randint(50, 500))
            data['status'].append('Completado')
            data['active'].append(False)
            data['created_at'].append(date)
            data['updated_at'].append(date)

    # Crear el DataFrame de órdenes
    orders = pd.DataFrame(data)


    try:
        orders.to_sql('orders', con=engine, if_exists='append', index=False)
        return {"message": "Inserción exitosa en la base de datos"}
    except sqlalchemy.exc.IntegrityError as e:
        return {"message": f"Error en la inserción: {str(e)}"}
    except Exception as e:
        return {"message": f"Error inesperado: {str(e)}"}

def predictOrdenes(dias_a_predecir: str):
    #Obtener dataset
    orders = fetch_orders_from_db()
    # Contar la cantidad de órdenes por día
    orders_per_day = orders['created_at'].value_counts().sort_index()

    # Crear un modelo ARIMA
    model = ARIMA(orders_per_day, order=(4, 0, 1))  # Ajustar los parámetros del modelo según corresponda

    # Entrenar el modelo
    model_fit = model.fit()

    # Realizar la predicción para los próximos 7 días
    fecha_actual = datetime.now() + timedelta(days=1)

    if dias_a_predecir == 'dias':
        # Diccionario para traducir los nombres de días
        dias_semana = {
            'monday': 'lunes',
            'tuesday': 'martes',
            'wednesday': 'miércoles',
            'thursday': 'jueves',
            'friday': 'viernes',
            'saturday': 'sábado',
            'sunday': 'domingo'
        }
        proximos_7_dias = [(fecha_actual + timedelta(days=i)).strftime("%A").lower() for i in range(7)]

        resultados = {}

        # Obtener las predicciones para los próximos 7 días
        for i, dia in enumerate(proximos_7_dias):
            prediccion = model_fit.forecast(steps=7)[i]  # Realizar la predicción para cada día
            dia_traducido = dias_semana[dia]  # Traducir el nombre del día
            resultados[dia_traducido] = prediccion

        return resultados

    elif dias_a_predecir == 'semanas':
        semanas = {}

        for i in range(4):
            semana = []
            for j in range(7):
                prediccion = model_fit.forecast(steps=1)[0]  # Predicción para el siguiente día
                semana.append(prediccion)
                model_fit = model_fit.append([prediccion], refit=False)  # Añadir la predicción al modelo

            # Calcular el total para la semana actual
            total_semana = sum(semana)

            semanas[f"Semana {i+1}"] = total_semana

            fecha_actual += timedelta(days=7)  # Mover a la siguiente semana

        return semanas
    
    elif dias_a_predecir == 'meses':
        # Diccionario para traducir los nombres de los meses
        meses_dict = {
            1: 'enero',
            2: 'febrero',
            3: 'marzo',
            4: 'abril',
            5: 'mayo',
            6: 'junio',
            7: 'julio',
            8: 'agosto',
            9: 'septiembre',
            10: 'octubre',
            11: 'noviembre',
            12: 'diciembre'
        }

        orders = fetch_orders_from_db()
        orders_per_day = orders['created_at'].value_counts().sort_index()

        model = ARIMA(orders_per_day, order=(4, 0, 1))
        model_fit = model.fit()

        fecha_actual = datetime.now() + timedelta(days=1)
        predicciones_meses = {}

        for i in range(6):
            mes = []
            # Se asume que cada mes tiene 30 días
            for j in range(30):
                prediccion = model_fit.forecast(steps=1)[0]
                mes.append(prediccion)
                model_fit = model_fit.append([prediccion], refit=False)

            total_mes = sum(mes)
            mes_actual = fecha_actual.month
            nombre_mes = meses_dict[mes_actual]
            predicciones_meses[nombre_mes] = total_mes

             # Manejo del cambio de año al llegar a diciembre
            if fecha_actual.month == 12:
                fecha_actual = fecha_actual.replace(year=fecha_actual.year + 1, month=1, day=1)
            else:
                fecha_actual = fecha_actual.replace(month=fecha_actual.month + 1, day=1)

        return predicciones_meses
    
    else:
        return {"message": "Parámetro incorrecto. Utiliza 'dias' o 'semanas'."}


@app.get("/admin/predecirOrdenes/{dias_a_predecir}")
def index(dias_a_predecir: str):
    return predictOrdenes(dias_a_predecir)

@app.get("/generar")
def index():
    return generate_sample_data()