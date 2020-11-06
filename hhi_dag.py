from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime

import psycopg2
import requests

dag = DAG(
	dag_id = 'hhi_dag',
	start_date = datetime(2020,11,6),
	schedule_interval = '0 11 * * *')



# Redshift connection 함수
def get_Redshift_connection():
    host = "grepp-data.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "hihigh49"
    redshift_pass = "Hihigh49!1"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True)
    return conn.cursor()

# 데이터 추출 및 로드 함수
def extract(url):
    f = requests.get(link)
    return (f.text)

def transform(text):
    lines = text.split("\n")
    return lines

def load(lines):
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    cur = get_Redshift_connection()
    for r in lines[1:]:
        if r != '':
            (name, gender) = r.split(",")
            print(name, "-", gender)
            sql = "BEGIN; TRUNCATE TABLE hihigh49.name_gender; INSERT INTO hihigh49.name_gender VALUES ('{name}', '{gender}'); END;".format(name=name, gender=gender)
            print(sql)
            cur.execute(sql)

def data_extract_load():
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    data = extract(link)
    lines = transform(data)
    load(lines)



# task 지정
redshift_connect = PythonOperator(
	task_id = 'redshift_connect',
	#python_callable param points to the function you want to run 
	python_callable = print_hello,
	#dag param points to the DAG that this task is a part of
	dag = dag)

data_extract_load = PythonOperator(
	task_id = 'data_extract_load',
	python_callable = data_extract_load,
	dag = dag)

#Assign the order of the tasks in our DAG
redshift_connect >> data_extract_load
