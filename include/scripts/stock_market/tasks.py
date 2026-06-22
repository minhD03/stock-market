import json
from io import BytesIO, StringIO

import psycopg2
import requests
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from minio import Minio

BUCKET_NAME = "stock-market"


def _get_postgres_connection():
    conn = psycopg2.connect(
        host="host.docker.internal",
        database="postgres",
        user="postgres",
        password="postgres",
    )
    return conn


def _get_minio_client():
    minio = BaseHook.get_connection("minio")
    client = Minio(
        endpoint=minio.extra_dejson["endpoint_url"].split("//")[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False,
    )
    return client


def _get_stock_prices(url, symbol):
    url = f"{url}{symbol}?interval=1d&range=4y"
    api = BaseHook.get_connection("stock_api")
    response = requests.get(url, headers=api.extra_dejson["headers"])
    return json.dumps(response.json()["chart"]["result"][0])


def _store_prices(stock):
    client = _get_minio_client()
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    stock = json.loads(stock)
    symbol = stock["meta"]["symbol"]
    data = json.dumps(stock, ensure_ascii=False).encode("utf-8")
    objw = client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f"{symbol}/prices.json",
        data=BytesIO(data),
        length=len(data),
        content_type="application/json",
    )
    return f"{objw.bucket_name}/{symbol}"


def _get_formatted_csv(stock_folder_path):
    client = _get_minio_client()
    prefix_name = f"{stock_folder_path.split('/')[1]}/formatted_prices/"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith(".csv"):
            return obj.object_name
    raise AirflowNotFoundException("The csv file does not exist in minio")


def _load_to_dw(csv_path):
    client = _get_minio_client()
    conn = _get_postgres_connection()
    cur = conn.cursor()

    response = client.get_object(bucket_name=BUCKET_NAME, object_name=csv_path)
    next(response)  # Skip the first line for the header

    cur.execute("CREATE SCHEMA IF NOT EXISTS dw;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dw.stock_market (
            "timestamp" bigint,
            close float,
            high float,
            low float,
            open float,
            volume bigint,
            date date
        );
    """)
    cur.execute("TRUNCATE TABLE dw.stock_market")
    copy_query = """
        COPY dw.stock_market (timestamp, close, high, low, open, volume, date)
        FROM STDIN
        WITH CSV
        DELIMITER ',';
    """
    cur.copy_expert(copy_query, StringIO(response.read().decode("utf-8")))
    conn.commit()

    cur.close()
    conn.close()
    response.close()
    response.release_conn()
