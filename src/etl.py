import datetime
import json
import logging
import os

import pandas as pd

import bigquery

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%X')


def load_and_transform_usage_data(client):
    df_started = bigquery.load_biqquery_data(client, "vm_assignments")
    df_shutdown = bigquery.load_biqquery_data(client, "workstation_shutdowns")
    df_stop_dmp = bigquery.load_biqquery_data(client, "workstation_stops_dmp")

    df_shutdown = pd.concat([df_shutdown, df_stop_dmp], ignore_index=True)
    df = pd.merge(df_started, df_shutdown, on=['user', 'instance_id'], how='left', suffixes=('_started', '_shutdown'))

    df["timestamp_shutdown"] = df.apply(insert_shutdown_time, axis=1)

    df = adjust_shutdown_times(df)

    df["usage_hours"] = (df["timestamp_shutdown"] - df["timestamp_started"]).dt.total_seconds() / 3600

    return df


def load_and_transform_tk_data(client):
    df_tk = bigquery.load_biqquery_data(client, "teamkatalogen")

    df_tk['members'] = df_tk['members'].apply(json.loads)
    df_tk = df_tk[df_tk['members'].str.len() > 0]

    # Normalize the JSON data
    df_members_normalized = pd.json_normalize(df_tk['members'].explode())[["navIdent", "roles"]]
    # Repeat the other columns to match the length of the normalized data
    df_tk_repeated = df_tk.loc[df_tk.index.repeat(df_tk['members'].str.len())].reset_index(drop=True)

    # Concatenate the repeated columns with the normalized data
    df_tk_normalized = pd.concat([df_tk_repeated.drop(columns=['members']), df_members_normalized], axis=1)

    # Choose first role in list
    df_tk_normalized["roles"] = df_tk_normalized["roles"].apply(lambda x: x[0] if isinstance(x, list) else x)

    #Join the usage data with the team data
    df_tk_normalized["navIdent"] = df_tk_normalized["navIdent"].str.lower()
    df_tk_normalized = df_tk_normalized.sort_values(['navIdent', 'teamtype']).drop_duplicates(subset='navIdent')

    return df_tk_normalized


def insert_shutdown_time(row):
    if pd.isna(row['timestamp_shutdown']):
        return row['timestamp_started'] + pd.Timedelta(2, 'h')
    return row['timestamp_shutdown']


def adjust_shutdown_times(df):
    df = df.sort_values(by=['user', 'timestamp_started']).reset_index(drop=True)

    for i in range(len(df)):
        # Adjust shutdown times to avoid overlap
        if i < len(df)-1:
            if df.iloc[i]['user'] == df.iloc[i+1]['user']:
                if df.iloc[i]['timestamp_shutdown'] > df.iloc[i+1]['timestamp_started']:
                    df.at[i, 'timestamp_shutdown'] = df.iloc[i+1]['timestamp_started'] - pd.Timedelta(1, 'm')
        # Adjust shutdown times to ensure no shutdowns in the future
        if df.iloc[i]['timestamp_shutdown'] > datetime.datetime.now(datetime.timezone.utc):
            df.at[i, 'timestamp_shutdown'] = datetime.datetime.now(datetime.timezone.utc)
    return df


def run_etl():
    logging.info("Starting ETL process")
    bq_client = bigquery.create_bigquery_client(os.getenv("GCP_PROJECT"))
    
    logging.info("Loading and transforming knast usage data")
    df_usage = load_and_transform_usage_data(bq_client)

    logging.info("Loading and transforming teamkatalogen data")
    df_tk = load_and_transform_tk_data(bq_client)

    df_merged = pd.merge(df_usage, df_tk, left_on='user', right_on='navIdent', how='left')

    df_bigquery = df_merged[["instance_id", "productarea", "roles", "timestamp_started", "timestamp_shutdown", "usage_hours"]]

    try: 
        table_id = os.getenv("GCP_PROJECT") + os.getenv("DATASET_ID") + os.getenv("TABLE_ID")
    except: 
        table_id = "nada-prod-6977.knast.knast_hours"

    logging.info(f"Writing data to {table_id}")
    bigquery.dataframe_to_bigquery(df_bigquery, bq_client, table_id, "WRITE_TRUNCATE")
    logging.info(f"{len(df_bigquery)} rows written to table {table_id}")
    
    logging.info("ETL process completed")