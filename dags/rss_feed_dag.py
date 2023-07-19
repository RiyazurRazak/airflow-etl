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
from azure.cosmos.aio import CosmosClient as cosmos_client


default_args = {
    "owner": "riyaz",
    "retries": 6
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
        logging.info(res.content)
        filename = "rss_feed_{}.xml".format(pendulum.now())
        open(filename, "x")
        file = open(filename, "w")
        file.write(res.text) 
        return filename

    @task  
    def transform(**context):
        ti = context["ti"]
        fileName = ti.xcom_pull(task_ids="extract",key="return_value")
        print(fileName)
        logging.info(fileName)
        tree = ET.parse(fileName)
        root = tree.getroot()
        items = []
        for item in root.findall('.//item'):
            title = item.find('title').text
            description = item.find('description').text
            guid = item.find("guid").text
            items.append((title, description, guid))
        curratedFilename = f'curated_{pendulum.now()}.csv'
        with open(curratedFilename, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Title', 'Description', 'GUID'])
            writer.writerows(items)
        return curratedFilename
        

    @task
    def load(**context):
        ti = context["ti"]
        filename = ti.xcom_pull(task_ids="transform",key="return_value")
        client = cosmos_client("url", credential="masterkey")
        database = client.get_database_client("etl")
        container = database.get_container_client("rssfeed")
        with open(filename, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for item in reader:
                container.upsert_item(item)

    extract() >> transform() >> load()


rss_feed_dag()


