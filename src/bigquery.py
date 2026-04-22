import os

from google.cloud.bigquery import Client, LoadJobConfig, SchemaField, Table


def create_bigquery_client(project):
    return Client(project=project)


def create_table(client, table_id, schema, exists_ok=False):
    table = Table(table_id, schema=schema)
    return client.create_table(table, exists_ok=exists_ok)


def load_biqquery_data(client, query_name):
    query_file_path = os.path.join(os.path.dirname(__file__), 'queries', f"{query_name}.sql")
    
    with open(query_file_path, "r") as file:
        query = file.read()
    return client.query(query).to_dataframe()


def dataframe_to_bigquery(df, client, table_id, write_disposition):
    tab_schema = client.get_table(table_id)
    job_config = LoadJobConfig(
        schema=tab_schema.schema, write_disposition=write_disposition
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


knast_hours_schema = [
    SchemaField("instance_id", "STRING", description="The instance ID of the VM"),
    SchemaField("productarea", "STRING"),
    SchemaField("roles", "STRING"),
    SchemaField("timestamp_started", "TIMESTAMP"),
    SchemaField("timestamp_shutdown", "TIMESTAMP"),
    SchemaField("usage_hours", "FLOAT"),
]


knast_last_used_schema = [
    SchemaField("uuid", "STRING", description="Random row identifier, regenerated on each run"),
    SchemaField("productarea", "STRING"),
    SchemaField("roles", "STRING"),
    SchemaField("last_used", "TIMESTAMP", description="Most recent session end across all sessions"),
    SchemaField("created_at", "DATE", description="Date the workstation config was created"),
    SchemaField("knast_exists", "BOOLEAN", description="Whether the workstation config still exists"),
    SchemaField("inactive", "BOOLEAN", description="Whether the user is no longer employed at NAV"),
]


knast_inactive_users_schema = [
    SchemaField("user", "STRING", description="Navident of the user"),
]


knast_configs_schema = [
    SchemaField("user", "STRING", description="Navident"),
    SchemaField("image", "STRING"),
    SchemaField("image_name", "STRING"),
    SchemaField("image_tag", "STRING"),
    SchemaField("machine_type", "STRING"),
    SchemaField("created_at", "DATE"),
    SchemaField("last_modified", "DATE"),
    SchemaField("version", "TIMESTAMP"),
    ]