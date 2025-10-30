from google.cloud import bigquery
from flask import Flask
from flask import request
import os 

app = Flask(__name__)
client = bigquery.Client()

@app.route('/')
def main(big_query_client=client):
    table_id = "gcp-end-to-end-471712.test_schema.us_states"
    schema = [
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("post_abbr", "STRING", mode="REQUIRED"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
    )
    uri = "gs://gcp-end-to-end-471712-demo-data-1761619421/us-states.csv"
    load_job = big_query_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )

    load_job.result()  

    destination_table = big_query_client.get_table(table_id)
    return {"data": destination_table.num_rows}

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))