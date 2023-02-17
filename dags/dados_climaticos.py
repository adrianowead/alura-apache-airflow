from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add

import os
import pandas as pd

with DAG(
    dag_id="dados_climaticos_praia_grande",
    start_date=pendulum.datetime(2023, 2, 9, tz="UTC"),
    schedule_interval='0 0 * * 1', # executar toda segunda feira
) as dag:

    tarefa_1 = BashOperator(
        task_id='cria_pasta',
        bash_command='mkdir -p "/storage/semana={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    def extrai_dados(data_interval_end: str):
        city = 'Praia Grande'
        key = os.getenv('VISUAL_CROSSING_KEY')

        URL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/%s/%s/%s?unitGroup=metric&include=days&key=%s&contentType=csv' % (city, data_interval_end, ds_add(data_interval_end, 7), key)

        dados = pd.read_csv(URL)

        fp = "/storage/semana=%s" % data_interval_end

        dados.to_csv(fp + 'dados_brutos.csv')
        dados[['datetime','tempmin','temp','tempmax']].to_csv(fp + 'temperatura.csv')
        dados[['datetime','description','icon']].to_csv(fp + 'condicoes.csv')

    tarefa_2 = PythonOperator(
        task_id='extrai_dados',
        python_callable=extrai_dados,
        op_kwargs={'data_interval_end':'{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    tarefa_1 >> tarefa_2