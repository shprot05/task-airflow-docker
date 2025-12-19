from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.datasets import Dataset
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
import os
import subprocess
import pandas as pd
import re


FINAL_FILE_PATH = '/opt/airflow/dags/data_frames/final_df.csv'
MY_DATASET = Dataset(f"file://{FINAL_FILE_PATH}")


def file_check_func():
    file_path = '/opt/airflow/dags/data/tiktok_google_play_reviews.csv'
    if os.path.getsize(file_path) == 0:
        subprocess.run('echo "BASH MSG: Файл пустой!"', shell=True)
    else:
        subprocess.run('echo "BASH MSG: Файл НЕ пустой!"', shell=True)


def clean_data_func():
    os.makedirs('/opt/airflow/dags/data_frames', exist_ok=True)

    df = pd.read_csv('/opt/airflow/dags/data/tiktok_google_play_reviews.csv')
    clean_df = df.fillna("-")

    output_path = '/opt/airflow/dags/data_frames/clean_df.csv'
    clean_df.to_csv(output_path, index=False)
    return output_path


def sort_data_func(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='data_processing_group.clean_data')

    df = pd.read_csv(file_path)
    sort_df = df.sort_values(by=['at'])

    output_path = '/opt/airflow/dags/data_frames/sort_df.csv'
    sort_df.to_csv(output_path, index=False)
    return output_path


def delete_symbols_func(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='data_processing_group.sort_data')
    df = pd.read_csv(file_path)

    def clean_text(text):
        if not isinstance(text, str):
            return ""
        pattern = r'[^a-zA-Zа-яА-ЯёЁ0-9\s\.,!?;:()"\'-]'
        return re.sub(pattern, '', text)

    if 'content' in df.columns:
        df['content'] = df['content'].apply(clean_text)

    df.to_csv(FINAL_FILE_PATH, index=False)
    print(f"Готово! Финальный файл сохранен: {FINAL_FILE_PATH}")
    return FINAL_FILE_PATH


# Функции второго дага
def load_to_mongo_func():
    if not os.path.exists(FINAL_FILE_PATH):
        raise FileNotFoundError("Файл dataset не найден!")

    df = pd.read_csv(FINAL_FILE_PATH)
    print(f"Загружаем {len(df)} строк в MongoDB...")

    data = df.to_dict(orient='records')
    hook = MongoHook(conn_id='mongo_default')

    hook.insert_many(
        mongo_collection='reviews',
        docs=data,
        mongo_db='analytics_db'
    )
    print("Данные успешно загружены.")



with DAG(
        dag_id="first_dag",
        start_date=datetime(2023, 12, 16),
        schedule_interval=None,
        catchup=False
) as dag1:
    file_sensor = FileSensor(
        task_id="file_sensor",
        filepath="/opt/airflow/dags/data/tiktok_google_play_reviews.csv",
        fs_conn_id='fs_default',
        poke_interval=5,
        timeout=60,
        mode="poke"
    )

    check_file_task = PythonOperator(
        task_id="task_after",
        python_callable=file_check_func
    )

    with TaskGroup("data_processing_group", tooltip="Этап обработки данных") as processing_group:
        clean_data = PythonOperator(
            task_id="clean_data",
            python_callable=clean_data_func
        )

        sort_data = PythonOperator(
            task_id="sort_data",
            python_callable=sort_data_func
        )

        delete_symbols = PythonOperator(
            task_id="delete_symbols",
            python_callable=delete_symbols_func,
            outlets=[MY_DATASET]
        )

        clean_data >> sort_data >> delete_symbols

    file_sensor >> check_file_task >> processing_group


with DAG(
        dag_id='second_dag_mongo_loader',
        start_date=datetime(2023, 1, 1),
        schedule=[MY_DATASET],
        catchup=False
) as dag2:
    load_task = PythonOperator(
        task_id="load_to_mongo",
        python_callable=load_to_mongo_func
    )