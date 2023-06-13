from airflow import DAG
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator




default_args = {
    'owner': 'Fernando Barbosa',
    'depends_on_past': False,
    'retries': 1,
    'start_date': datetime(2023, 1, 1),
    'catchup': False,
    'description': 'estudos_GCP - ETL: Dados de Exportação de Carnes'
}


UPLOAD_FILE_PATH = r'C:\Users\BlueShift\Desktop\Portfolio - Fer\fer_airflow\data\dados_brutos.parquet'
BUCKET_NAME = "fernando-barbosa"
FILE_NAME =  "projeto_01/01_bronze/dados_bronze.parquet"



gcp_project_id = Variable.get('gcp_project_id')
bucket = Variable.get('bucket_gcp')
cluster = Variable.get('cluster_dataproc')
regiao = Variable.get('regiao_gcp')
location = ''
py_spark_notebook = Variable.get('py_spark_notebook')


def get_dados():

    url_quad_2023 = r'http://api.comexstat.mdic.gov.br/general?filter=%7B%22yearStart%22:%222023%22,%22yearEnd%22:%222023%22,%22typeForm%22:1,%22typeOrder%22:2,%22filterList%22:%5B%7B%22id%22:%22noPaispt%22,%22text%22:%22Pa%C3%ADs%22,%22route%22:%22/pt/location/countries%22,%22type%22:%221%22,%22group%22:%22gerais%22,%22groupText%22:%22Gerais%22,%22hint%22:%22fieldsForm.general.noPais.description%22,%22placeholder%22:%22Pa%C3%ADses%22%7D,%7B%22id%22:%22noUf%22,%22text%22:%22UF%20do%20Produto%22,%22route%22:%22/pt/location/states%22,%22type%22:%221%22,%22group%22:%22gerais%22,%22groupText%22:%22Gerais%22,%22hint%22:%22fieldsForm.general.noUf.description%22,%22placeholder%22:%22UFs%20do%20Produto%22%7D,%7B%22id%22:%22noNcmpt%22,%22text%22:%22NCM%20-%20Nomenclatura%20Comum%20do%20Mercosul%22,%22route%22:%22/pt/product/ncm%22,%22type%22:%222%22,%22group%22:%22sh%22,%22groupText%22:%22Sistema%20Harmonizado%20(SH)%22,%22hint%22:%22fieldsForm.general.noNcm.description%22,%22placeholder%22:%22NCM%22%7D,%7B%22id%22:%22noUrf%22,%22text%22:%22URF%22,%22route%22:%22/pt/location/urf%22,%22type%22:%221%22,%22group%22:%22gerais%22,%22groupText%22:%22Gerais%22,%22hint%22:%22fieldsForm.general.noUrf.description%22,%22placeholder%22:%22URFs%22%7D,%7B%22id%22:%22noVia%22,%22text%22:%22Via%22,%22route%22:%22/pt/location/via%22,%22type%22:%221%22,%22group%22:%22gerais%22,%22groupText%22:%22Gerais%22,%22hint%22:%22fieldsForm.general.noVia.description%22,%22placeholder%22:%22Vias%22%7D,%7B%22id%22:%22noBlocopt%22,%22text%22:%22Bloco%20Econ%C3%B4mico%22,%22route%22:%22/pt/location/blocks%22,%22type%22:%221%22,%22group%22:%22gerais%22,%22groupText%22:%22Gerais%22,%22hint%22:%22fieldsForm.general.noBloco.description%22,%22placeholder%22:%22Blocos%20Econ%C3%B4micos%22%7D%5D,%22filterArray%22:%5B%7B%22item%22:%5B%5D,%22idInput%22:%22noPaispt%22%7D,%7B%22item%22:%5B%5D,%22idInput%22:%22noUf%22%7D,%7B%22item%22:%5B%2202011000%22,%2202012010%22,%2202012020%22,%2202012090%22,%2202013000%22,%2202021000%22,%2202022010%22,%2202022020%22,%2202022090%22,%2202023000%22,%2202061000%22,%2202062100%22,%2202062200%22,%2202062910%22,%2202062990%22%5D,%22idInput%22:%22noNcmpt%22%7D,%7B%22item%22:%5B%5D,%22idInput%22:%22noUrf%22%7D,%7B%22item%22:%5B%5D,%22idInput%22:%22noVia%22%7D,%7B%22item%22:%5B%5D,%22idInput%22:%22noBlocopt%22%7D%5D,%22rangeFilter%22:%5B%5D,%22detailDatabase%22:%5B%7B%22id%22:%22noPaispt%22,%22text%22:%22Pa%C3%ADs%22%7D,%7B%22id%22:%22noUf%22,%22text%22:%22UF%20do%20Produto%22%7D,%7B%22id%22:%22noNcmpt%22,%22text%22:%22NCM%20-%20Nomenclatura%20Comum%20do%20Mercosul%22,%22parentId%22:%22coNcm%22,%22parent%22:%22C%C3%B3digo%20NCM%22%7D,%7B%22id%22:%22noUrf%22,%22text%22:%22URF%22%7D,%7B%22id%22:%22noVia%22,%22text%22:%22Via%22%7D,%7B%22id%22:%22noBlocopt%22,%22text%22:%22Bloco%20Econ%C3%B4mico%22%7D%5D,%22monthDetail%22:true,%22metricFOB%22:true,%22metricKG%22:true,%22metricStatistic%22:false,%22monthStart%22:%2201%22,%22monthEnd%22:%2212%22,%22formQueue%22:%22general%22,%22langDefault%22:%22pt%22,%22monthStartName%22:%22Janeiro%22,%22monthEndName%22:%22Dezembro%22%7D'

    headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,la;q=0.6',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json',
    'Origin': 'http://comexstat.mdic.gov.br',
    'Referer': 'http://comexstat.mdic.gov.br/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
    }


    resp = requests.get(
            url_quad_2023,
            headers=headers,
            verify=False,
        )
    print('Status da requisição 2023:')
    print(resp.status_code)

    resp_dict = json.loads(resp.text)
    dados = resp_dict['data']
    df_2023 = pd.json_normalize(dados['list'])

    print(df_2023.shape)

    df_2023.to_parquet(r'C:\Users\BlueShift\Desktop\Portfolio - Fer\fer_airflow\data\dados_brutos.parquet', index=False)




with DAG(
    "etl_export_gcp",
    default_args = default_args,
    schedule_interval=None,
) as dag:

    inicio_dag = DummyOperator(task_id="inicio_dag")

    coleta_de_dados = PythonOperator(
        task_id = 'coleta_de_dados',
        python_callable = get_dados
    )

 

    envio_gcs = LocalFilesystemToGCSOperator(
        task_id="envio_gcs",
        src=UPLOAD_FILE_PATH,
        dst=FILE_NAME,
        bucket=BUCKET_NAME,
        gcp_conn_id="gcp",
    )

   
    PYSPARK_JOB = {
        "reference": {"project_id": gcp_project_id},
        "placement": {"cluster_name": cluster},
        "pyspark_job": {"main_python_file_uri": py_spark_notebook},
    }



    tratamento_pyspark = DataprocSubmitJobOperator(
        task_id = "tratamento_pyspark", 
        job = PYSPARK_JOB, 
        region = regiao, 
        project_id = gcp_project_id,
        asynchronous = True,
        gcp_conn_id = 'gcp'
)
    fim_dag = DummyOperator(task_id="fim_dag")


inicio_dag >> coleta_de_dados >>  envio_gcs >> tratamento_pyspark >> fim_dag
