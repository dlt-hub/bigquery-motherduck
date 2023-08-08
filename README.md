# BigQuery -> MotherDuck data pipeline

GitHub repo accompanying [this](link-to-blog) blog. This repo contains code for a `dlt` pipeline that loads data from BigQuery to MotherDuck, and a dbt package that performs transformations on the loaded data and writes the transformed data to the same database. 
  
**Background**: Google Analytics 4 (GA4) only makes it possible to export data to BigQuery. This means that if you want to combine your GA4 data with other data and do your own analytics on top of it, you will need to create a data pipeline that can either source data directly from GA4 or source data from BigQuery. 

`dlt` has an existing [Google Analytics 4 pipeline](https://dlthub.com/docs/dlt-ecosystem/verified-sources/google_analytics) that can load data directly from GA4.  
  
In this demo, however, I will load GA4 data from BigQuery instead by writing a BigQuery pipeline from scratch.  

**Pre-requisites:** 
1. GCP service account with Google Analytics and BigQuery enabled
2. GA4 data exported to BigQuery
3. BigQuery service account credentials
  
## How to run this pipeline

1. Clone this repo.
2. Inside the folder `.dlt`, create a file called `secrets.toml` and add service account credentials for your BigQuery project and the credentials for MotherDuck as below(see [here](https://dlthub.com/docs/dlt-ecosystem/destinations/motherduck#setup-guide) for more details):
    ```
    [source.bigquery.credentials]
    project_id = "project_id" # please set me up!
    private_key = "private_key" # please set me up!
    client_email = "client_email" # please set me up!
    token_uri = "token_uri" # please set me up!
    location = "location" # please set me up!

    [destination.motherduck.credentials]
    database = "database name" # please set me up!
    password = "token" # please set me up!
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
  
### Deploying this pipeline  
  
After running this pipeline successfully, you can deploy this pipeline as follows:  
1. Create a GitHub repository for your `dlt` project if not already created.  
2. Run `dlt deploy --schedule "0 0 1 * *" bigquery.py github action`  
    This will schedule your pipeline to run on the first day of every month. 
3. Finally add and commit your files and push them to GitHub  
    `git add . && git commit -m 'pipeline deployed with github action'`  
    `git push origin`

See [here](https://dlthub.com/docs/walkthroughs/deploy-a-pipeline/deploy-with-github-actions) for details.

## How this pipeline was created  

This pipeline was created using the BigQuery -> DuckDB pipeline: see [here](https://github.com/dlt-hub/bigquery-motherduck/tree/main/bigquery_duckdb) for full details. 
  
To create a BigQuery -> MotherDuck pipeline, follow all the steps above, but instead of passing 'duckdb' as a destination, pass 'motherduck' as a destination. Also add MotherDuck credentials inside `.dlt/secrets.toml` as above.
  
