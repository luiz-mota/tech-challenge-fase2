import os

import pandas as pd
from google.cloud import bigquery

from src.utils.gcs_helpers import get_credentials


def get_bigquery_client() -> bigquery.Client:
    credentials = get_credentials(["https://www.googleapis.com/auth/bigquery"])
    return bigquery.Client(credentials=credentials, project=os.environ["GCP_PROJECT_ID"])


def upload_dataframe(df: pd.DataFrame, dataset_id: str, table_id: str) -> None:
    client = get_bigquery_client()
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)

    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
