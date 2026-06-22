from datetime import datetime

import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.slack.notifications.slack import SlackNotifier
from airflow.sensors.base import PokeReturnValue

from include.scripts.stock_market.tasks import (
    # BUCKET_NAME,
    _get_formatted_csv,
    _get_stock_prices,
    _load_to_dw,
    _store_prices,
)

SYMBOL = "INTC"
# AAPL, MSFT, GOOGL, AMZN, TSLA, META, NFLX, NVDA, AMD, INTC


@dag(
    start_date=datetime(2022, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["stock_market"],
    on_success_callback=SlackNotifier(
        slack_conn_id="slack",
        text="Stock market DAG completed successfully",
        channel="#stock-pipeline",
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id="slack",
        text="Stock market DAG failed",
        channel="#stock-pipeline",
    ),
    # default_args=default_args,
)
def stock_market():
    @task.sensor(poke_interval=30, timeout=300, mode="poke")
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection("stock_api")
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        response = requests.get(url, headers=api.extra_dejson["headers"])
        condition = response.json()["finance"]["result"] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)

    get_stock_prices = PythonOperator(
        task_id="get_stock_prices",
        python_callable=_get_stock_prices,
        op_kwargs={
            "url": "{{ ti.xcom_pull(task_ids='is_api_available') }}",
            "symbol": SYMBOL,
        },
    )

    store_prices = PythonOperator(
        task_id="store_prices",
        python_callable=_store_prices,
        op_kwargs={"stock": "{{ ti.xcom_pull(task_ids='get_stock_prices') }}"},
    )

    format_prices = DockerOperator(
        task_id="format_prices",
        image="spark-app",
        container_name="format_prices",
        api_version="auto",
        auto_remove=True,
        docker_url="tcp://docker-proxy:2375",
        network_mode="container:spark-master",
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            "SPARK_APPLICATION_ARGS": "{{ ti.xcom_pull(task_ids='store_prices') }}"
        },
    )

    get_formatted_csv = PythonOperator(
        task_id="get_formatted_csv",
        python_callable=_get_formatted_csv,
        op_kwargs={
            "stock_folder_path": "{{ ti.xcom_pull(task_ids='store_prices') }}",
        },
    )

    load_to_dw = PythonOperator(
        task_id="load_to_dw",
        python_callable=_load_to_dw,
        op_kwargs={
            "csv_path": "{{ ti.xcom_pull(task_ids='get_formatted_csv') }}",
        },
    )

    (
        is_api_available()
        >> get_stock_prices
        >> store_prices
        >> format_prices
        >> get_formatted_csv
        >> load_to_dw
    )


stock_market()
