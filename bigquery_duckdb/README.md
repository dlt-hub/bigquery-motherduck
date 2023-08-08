# BigQuery -> DuckDB data pipeline

GitHub repo accompanying [this](link-to-blog) blog. This repo contains code for a `dlt` pipeline that loads data from BigQuery to DuckDB, and a dbt package that performs transformations on the loaded data and writes the transformed data to the same database.  
  
**Background**: Google Analytics 4 (GA4) only makes it possible to export data to BigQuery. This means that if you want to combine your GA4 data with other data and do your own analytics on top of it, you will need to create a data pipeline that can either source data directly from GA4 or source data from BigQuery. 

`dlt` has an existing [Google Analytics 4 pipeline](https://dlthub.com/docs/dlt-ecosystem/verified-sources/google_analytics) that can load data directly from GA4.  
  
In this demo, however, I will load GA4 data from BigQuery instead by writing a a BigQuery pipeline from scratch.  

**Pre-requisites:** 
1. GCP service account with Google Analytics and BigQuery enabled
2. GA4 data exported to BigQuery
3. BigQuery service account credentials

## How to run this pipeline

1. Clone this repo.
2. Inside the folder `.dlt`, create a file called `secrets.toml` and add the credentials for your BigQuery project as below:
    ```
    [source.bigquery.credentials]
    project_id = "project_id" # please set me up!
    private_key = "private_key" # please set me up!
    client_email = "client_email" # please set me up!
    token_uri = "token_uri" # please set me up!
    location = "location" # please set me up!
    ```
3. Modify `bigquery.py`:  
   Inside the resource function `bigquery_resource`, modify the query string by replacing `"project_name.dataset_name.events_*"` with the name of your dataset in BigQuery.
   ```
       query_str = f"""
        select * from `project_name.dataset_name.events_*` 
        where _table_suffix between format_date('%Y%m%d',cast('{year}-{month}-1' as date)) 
                                and format_date('%Y%m%d',date_add(cast('{year}-{month}-1' as date), interval 1 month))
    """
   ```
   
4. Install requirements:  
```pip install -r requirements.txt```
5. Run the pipeline:  
```python bigquery.py```

This will create a file called `bigquery_pipeline.duckdb` in your working directory which will contain the loaded and transformed data.

## How this pipeline was created

### Creating a `dlt` project
1. Install `dlt`:  
    ```pip install dlt```

2. Create a `dlt` project using `dlt init` and specifying the source and destination (See [here](https://dlthub.com/docs/walkthroughs/create-a-pipeline) for details):  
    ```dlt init bigquery duckdb```  
    This will create a directory with the structure:  
    ```
    ├── .dlt
    │   ├── config.toml
    │   └── secrets.toml
    ├── bigquery.py
    └── requirements.txt
    ```

3. Inside `.dlt/secrets.toml`, add service account credentials for your BigQuery project as below:  
    ```
    [source.bigquery.credentials]
        project_id = "project_id" # please set me up!
        private_key = "private_key" # please set me up!
        client_email = "client_email" # please set me up!
        token_uri = "token_uri" # please set me up!
        location = "location" # please set me up!
    ```

4. Modify the function `bigquery_resource` in `bigquery.py`. This is the function that will request data from the API and return the output, hence you would need to pass credentials and make API requests inside this function. (See [here](https://dlthub.com/docs/walkthroughs/create-a-pipeline#3-request-data-from-the-weatherapicom-api) for details). Add the following inside `bigquery_resource` changes are made to `bigquery.py`
    1. Pass service account credentials
    ```
        service_account_info = {
            "project_id": dlt.secrets.get('source.bigquery.credentials.project_id'),
            "private_key": dlt.secrets.get('source.bigquery.credentials.private_key'),
            "client_email": dlt.secrets.get('source.bigquery.credentials.client_email'),
            "token_uri": dlt.secrets.get('source.bigquery.credentials.token_uri'),
            "location": dlt.secrets.get('source.bigquery.credentials.location'),
        }
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        client = bigquery.Client(credentials=credentials)
    ```  

    Using `dlt.secrets.get` you can fetch the credentials from `.dlt/secrets.toml`.  

    2. Query the BigQuery API to get GA4 data for a period of 1 month
    ```
        query_str = f"""
            select * from <ga_events_table>
            where _table_suffix between format_date('%Y%m%d',cast('{year}-{month}-1' as date)) 
                                    and format_date('%Y%m%d',date_add(cast('{year}-{month}-1' as date), interval 1 month))
        """

        for row in tqdm.tqdm(client.query(query_str)):
            yield {key:value for key,value in row.items()}
    ```
5. With the resource function written you can now run the pipeline. But first install all dependencies using `pip install -r requirements.txt`. By default, `requirements.txt` will only contain the dependencies needed for the `dlt` environment, so you will need to install any other dependenies needed by `bigquery.py`.  
  
6. Finally, run the pipeline using `python bigquery.py`. This will create a file `bigquery_pipeline.duckdb` in your working directory containing the data queried from BigQuery.

### Creating a dbt project  
  
You can integrate dbt directly into the pipeline created above:  
  
1. Install dbt using `pip install dbt-duckdb`  
2. Create a dbt project inside the `dlt` project using `dbt init dbt_bigquery`  
3. Add your model inside the dbt project as usual. The transformations are going to be done on the data loaded by `dlt`, and hence the dataset and table names should match those of the .duckdb file created above.  
4. Add the `dlt` dbt runner inside `bigquery.py` (See [here](https://dlthub.com/docs/dlt-ecosystem/transformations/dbt#how-to-use-the-dbt-runner) for details)  
5. Running `python bigquery.py` will first load data from BigQuery into a DuckDB dataset, perform transformations, and store the data in the same dataset.


