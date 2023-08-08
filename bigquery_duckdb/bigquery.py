import dlt
from dlt.sources.helpers import requests

import pendulum
import tqdm
from google.cloud import bigquery
from google.oauth2 import service_account


today = pendulum.now()
if today.month > 1:
    last_month = today.month - 1
    year = today.year
else:
    last_month = 12
    year = today.year - 1

@dlt.source
def bigquery_source(api_secret_key=dlt.secrets.value):
    return bigquery_resource(api_secret_key)


def _create_auth_headers(api_secret_key):
    """Constructs Bearer type authorization header which is the most common authorization method"""
    headers = {"Authorization": f"Bearer {api_secret_key}"}
    return headers


@dlt.resource(write_disposition="append")
def bigquery_resource(month=last_month, year=year):

    service_account_info = {
        "project_id": dlt.secrets.get('source.bigquery.credentials.project_id'),
        "private_key": dlt.secrets.get('source.bigquery.credentials.private_key'),
        "client_email": dlt.secrets.get('source.bigquery.credentials.client_email'),
        "token_uri": dlt.secrets.get('source.bigquery.credentials.token_uri'),
        "location": dlt.secrets.get('source.bigquery.credentials.location'),
    }
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    client = bigquery.Client(credentials=credentials)

    query_str = f"""
        select * from `dlthub-analytics.analytics_345886522.events_*` 
        where _table_suffix between format_date('%Y%m%d',cast('{year}-{month}-1' as date)) 
                                and format_date('%Y%m%d',date_add(cast('{year}-{month}-1' as date), interval 1 day))
    """

    print('loading rows')
    for row in tqdm.tqdm(client.query(query_str)):
        yield {key:value for key,value in row.items()}



if __name__ == "__main__":

    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name='bigquery_pipeline', destination='duckdb', dataset_name='bigquery_data'
    )

    # print credentials by running the resource
    data = list(bigquery_resource())

    # print the data yielded from resource
    # print(data)

    # run the pipeline with your parameters
    load_info = pipeline.run(data,table_name="events")

    # pretty print the information on data that was loaded
    print(load_info)

    # perform transformations on the data loaded by dlt
    # store the transformed data in the dataset 'page_view_events'
    pipeline = dlt.pipeline(
        pipeline_name='bigquery_pipeline',
        destination='duckdb',
        dataset_name='page_view_events'
    )

    # get venv for dbt
    venv = dlt.dbt.get_venv(pipeline)

    # get runner
    dbt = dlt.dbt.package(
        pipeline,
        "dbt_bigquery",
        venv=venv
    )

    # run the dbt models
    models = dbt.run_all()

    for m in models:
        print(
            f"Model {m.model_name} materialized" +
            f"in {m.time}" +
            f"with status {m.status}" +
            f"and message {m.message}"
        )
