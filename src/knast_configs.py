import logging
import os

import pandas as pd
from google.cloud import workstations_v1

import bigquery


def list_workstation_configs(project_id, cluster_id):
    client = workstations_v1.WorkstationsClient()
    parent = f"projects/{project_id}/locations/europe-north1/workstationClusters/{cluster_id}"
    configs = client.list_workstation_configs(parent=parent)
    
    return configs


def unpack_configs(configs):
    all_configs = []
    current_time = pd.Timestamp.now()
    for config in configs:
        d = {}
        d["user"] = config.labels["subject-ident"]
        d["image"] = config.container.image
        d["image_name"] = config.container.image.split("/")[-1].split(":")[0]
        d["image_tag"] = config.container.image.split(":")[-1]
        d["machine_type"] = config.host.gce_instance.machine_type
        d["created_at"] = config.create_time.date()
        d["last_modified"] = config.update_time.date()
        d["version"] = current_time
        all_configs.append(d)

    return pd.DataFrame(all_configs)


def run_knast_configs_etl():
    logging.info("Starting ETL process for knast configs")
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    
    logging.info("Listing knast configs")
    configs = list_workstation_configs("knada-gcp", "knada")

    logging.info("Unpacking knast configs")
    df_configs = unpack_configs(configs)

    bq_client = bigquery.create_bigquery_client(project_id)
    table_id = f"{project_id}.knast.knast_configs"

    logging.info(f"Writing data to {table_id}")
    bigquery.dataframe_to_bigquery(df_configs, bq_client, table_id, "WRITE_APPEND")
    logging.info(f"{len(df_configs)} rows written to table {table_id}")
    
    logging.info("ETL process for knast configs completed")