from google.cloud import bigquery
import google.cloud.exceptions

def get_improved_bad_request_exception(
    job: google.cloud.bigquery.job.LoadJob
) -> google.cloud.exceptions.BadRequest:
    errors = job.errors
    result = google.cloud.exceptions.BadRequest(
        '; '.join([error['message'] for error in errors]),
        errors=errors
    )
    result._job = job
    return result


def upload_exposed_services_file(table_id, file_path):
    # https://cloud.google.com/bigquery/docs/samples/bigquery-create-table

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, 
        # skip_leading_rows=1, 
        # source_format=bigquery.SourceFormat.CSV, 
        # skip_leading_rows=1, 
        # autodetect=True,
        schema=[
        bigquery.SchemaField("ip", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("asn", "INT64"),
        bigquery.SchemaField("dns_name", "STRING", "REPEATED"),
        bigquery.SchemaField("port", "INT64", "REPEATED"),
        bigquery.SchemaField("pep_link", "BOOL", "REPEATED"),
        bigquery.SchemaField("stop_reason", "STRING"),
        bigquery.SchemaField("hop_count", "FLOAT"),
        bigquery.SchemaField("sec_last_ip", "STRING"),
        bigquery.SchemaField("sec_last_hop", "FLOAT"),
    ],
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    try:
        job.result()
    except google.cloud.exceptions.BadRequest as exc:
        raise get_improved_bad_request_exception(job) from exc

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def upload_ping_file(table_id, file_path):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, 
        # skip_leading_rows=1, 
        # autodetect=True,
        schema=[
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("seq", "INT64"),
        bigquery.SchemaField("dst", "STRING"),
        bigquery.SchemaField("stop_reason", "STRING"),
        bigquery.SchemaField("start_time", "TIMESTAMP"),
        bigquery.SchemaField("start_sec", "INT64"),
        bigquery.SchemaField("hop_count", "FLOAT64"),
        bigquery.SchemaField("ip_at_ttl", "STRING"),
        bigquery.SchemaField("probe_ttl", "FLOAT64"),
        bigquery.SchemaField("rtt", "FLOAT64"),
    ],
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )