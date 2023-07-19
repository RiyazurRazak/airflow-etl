import json
import pendulum
import logging
import xml.etree.ElementTree as ET
import csv
from airflow.decorators import (
    dag,
    task,
) 
import requests
from pymongo import MongoClient
import os


default_args = {
    "owner": "riyaz",
    "retries": 3
}

@dag(
    dag_id="rss_feed",
    schedule="0 23 * * *",
    start_date=pendulum.datetime(2023, 7, 19),
    catchup=False,
    tags=["rss"],
    default_args=default_args
)
def rss_feed_dag():
    @task
    def extract(**context):
        res = requests.get("https://timesofindia.indiatimes.com/rssfeedstopstories.cms")
        if not os.path.exists("raw"):
            os.mkdir("raw")
        filename = f"raw/raw_rss_feed_{pendulum.now()}.xml"
        with open(filename, "w") as file:
            file.write(res.text) 
        return filename

    @task  
    def transform(**context):
        ti = context["ti"]
        fileName = ti.xcom_pull(task_ids="extract",key="return_value")
        tree = ET.parse(fileName)
        root = tree.getroot()
        items = []
        for item in root.findall('.//item'):
            title = item.find('title').text
            description = item.find('description').text
            guid = item.find("guid").text
            items.append((title, description, guid))
        if not os.path.exists("curated"):
            os.mkdir("curated")
        curratedFilename = f'curated/curated_{pendulum.now()}.csv'
        with open(curratedFilename, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Title', 'Description', 'GUID'])
            writer.writerows(items)
        return curratedFilename
        

    @task
    def load(**context):
        ti = context["ti"]
        filename = ti.xcom_pull(task_ids="transform",key="return_value")
        mongoClient = MongoClient("<connection_string>")
        db = mongoClient['warehouse']
        collection = db['rss_feed']
        with open(filename, newline='') as csvfile:
            items = csv.DictReader(csvfile)
            collection.insert_many(items)

    extract() >> transform() >> load()


rss_feed_dag()


