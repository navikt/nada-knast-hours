import logging
import os
import uuid

import pandas as pd

import bigquery
from knast_hours import load_and_transform_usage_data, load_and_transform_tk_data


def load_workstation_configs(client):
    return bigquery.load_biqquery_data(client, "knast_configs_history")


def run_knast_last_used_etl():
    logging.info("Starting ETL process for knast last used")
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    bq_client = bigquery.create_bigquery_client(project=project_id)

    logging.info("Loading and transforming knast usage data")
    df_usage = load_and_transform_usage_data(bq_client)

    logging.info("Loading workstation configs from BQ history")
    df_configs = load_workstation_configs(bq_client)

    logging.info("Loading and transforming teamkatalogen data")
    df_tk = load_and_transform_tk_data(bq_client)

    logging.info("Loading personer data")
    df_personer = bigquery.load_biqquery_data(bq_client, "personer")
    df_personer["navident"] = df_personer["navident"].str.lower()
    df_personer["inactive"] = df_personer["inactive"].str.lower() == "true"

    df_last_used = (
        df_usage.groupby("user", as_index=False)["timestamp_shutdown"]
        .max()
        .rename(columns={"timestamp_shutdown": "last_used"})
    )

    df_last_used = pd.merge(df_last_used, df_configs, on="user", how="left")

    df_last_used = pd.merge(df_last_used, df_tk, left_on="user", right_on="navIdent", how="left")
    df_last_used = pd.merge(df_last_used, df_personer[["navident", "inactive"]], left_on="user", right_on="navident", how="left")

    df_last_used["uuid"] = [str(uuid.uuid4()) for _ in range(len(df_last_used))]

    df_bigquery = df_last_used[["uuid", "productarea", "roles", "last_used", "created_at", "knast_exists", "inactive"]]

    table_id = f"{project_id}.knast.knast_last_used"

    logging.info(f"Ensuring table {table_id} exists")
    bigquery.create_table(bq_client, table_id, bigquery.knast_last_used_schema, exists_ok=True)

    logging.info(f"Writing data to {table_id}")
    bigquery.dataframe_to_bigquery(df_bigquery, bq_client, table_id, "WRITE_TRUNCATE")
    logging.info(f"{len(df_bigquery)} rows written to table {table_id}")

    inactive_table_id = f"{project_id}.knast.knast_inactive_users"
    df_inactive = df_last_used.loc[
        (df_last_used["knast_exists"] == True) & (df_last_used["inactive"] == True),
        ["user"]
    ]

    logging.info(f"Ensuring table {inactive_table_id} exists")
    bigquery.create_table(bq_client, inactive_table_id, bigquery.knast_inactive_users_schema, exists_ok=True)

    logging.info(f"Writing {len(df_inactive)} inactive user(s) to {inactive_table_id}")
    bigquery.dataframe_to_bigquery(df_inactive, bq_client, inactive_table_id, "WRITE_TRUNCATE")

    logging.info("ETL process for knast last used completed")
