from datetime import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
#import uuid

default_args = {
    'owner': 'leticia',
    'start_date': dt(2023, 9, 3, 10, 00)
}


def get_crypto_price(coin):
    import requests

    headers = {
        'x-access-token': 'my_secret_token'
    }

    currency = 'n5fpnvMGNsOS'  # reais
    url = f"https://api.coinranking.com/v2/coin/{coin}/price?referenceCurrencyUuid={currency}"
    response = requests.request("GET", url=url, headers=headers)
    data = response.json()['data']

    return data


def format_data(data, coin_name):
    data_final = {}

    # data_final['id'] = str(uuid.uuid4())
    data_final['ts_ingestion'] = str(dt.fromtimestamp(data['timestamp']))
    data_final['coin'] = coin_name
    data_final['price'] = format(float(data['price']), ".2f")

    return data_final


def stream_data():
    import json
    from kafka import KafkaProducer
    import logging

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    coins = ['Qwsogvtv82FCd', 'razxDUgYGNAdQ', 'HIVsRcGKkPFtW']
    names = ['BTC', 'ETH', 'USDT']

    try:
        for coin, name in zip(coins, names):
            data = get_crypto_price(coin)
            data = format_data(data, name)

            producer.send('bitcoin_stream', json.dumps(data).encode('utf-8'))

    except Exception as e:
        logging.error(f'Error: {e}')


with DAG('bitcoin_stream',
         default_args=default_args,
         schedule_interval='* * * * *',
         catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
