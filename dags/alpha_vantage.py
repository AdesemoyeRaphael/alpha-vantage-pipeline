import pandas as pd
import json
from airflow.sdk import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3ReadObjectOperator,
)
from transform import parse_trade_records, validate_trade_data
from datetime import datetime, timedelta

default_args = {"owner": "raph", "retries": 1, "retry_delay": timedelta(minutes=5)}
bucket_name = "my-project-alpha-vantage"
stock_type = "apple"


@dag(
    dag_id="alphave_vantage",
    default_args=default_args,
    description="Project 1",
    start_date=datetime(2026, 7, 24),
    schedule="@daily",
    catchup=False,
)
def alpha_vantage_func():
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="snowflake_con",
        sql="""
            CREATE TABLE IF NOT EXISTS ALPHA_VANTAGE_APPLE (
            id INT AUTOINCREMENT,
            date DATE PRIMARY KEY,
            open DECIMAL(10, 4) NOT NULL,
            close DECIMAL(10, 4) NOT NULL,
            high DECIMAL(10, 4) NOT NULL,
            low DECIMAL(10, 4) NOT NULL,
            volume INT NOT NULL,
            daily_pct_change DECIMAL(10, 4) NOT NULL
            );
        """,
    )

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        aws_conn_id="aws_con",
        bucket_name=bucket_name,
        region_name="us-east-1",
    )

    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="alpha_vantage_api",
        endpoint="query?function=TIME_SERIES_DAILY&symbol=AAPL&apikey={{ var.value.alpha_vantage_api_key }}",
    )

    extract_data = HttpOperator(
        task_id="extract_data",
        http_conn_id="alpha_vantage_api",
        method="GET",
        endpoint="query?function=TIME_SERIES_DAILY&symbol=AAPL&apikey={{ var.value.alpha_vantage_api_key }}",
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )

    @task()
    def _prepare_data(raw_data):
        time_series = raw_data.get("Time Series (Daily)")
        result = []
        for k, v in time_series.items():
            key_name = f"{stock_type}/{k.replace('-', '/')}.json"
            result.append(
                {
                    "s3_key": key_name,
                    "bucket_key": key_name,
                    "data": json.dumps(v, indent=2),
                    "date": k,
                }
            )
        return result

    prepared = _prepare_data(extract_data.output)

    save_to_s3 = S3CreateObjectOperator.partial(
        task_id="save_to_s3",
        aws_conn_id="aws_con",
        s3_bucket=bucket_name,
        replace=True,
        region_name="us-east-1",
    ).expand_kwargs(prepared.map(lambda p: {"s3_key": p["s3_key"], "data": p["data"]}))

    is_raw_data_save = S3KeySensor.partial(
        task_id="is_raw_data_save",
        aws_conn_id="aws_con",
        bucket_name=bucket_name,
        timeout=300,
        poke_interval=30,
    ).expand_kwargs(prepared.map(lambda p: {"bucket_key": p["bucket_key"]}))

    read_s3_data = S3ReadObjectOperator.partial(
        task_id="read_s3_data",
        aws_conn_id="aws_con",
        s3_bucket=bucket_name,
        region_name="us-east-1",
    ).expand_kwargs(prepared.map(lambda p: {"s3_key": p["s3_key"]}))

    @task()
    def _process_data(keys, contents):
        df = parse_trade_records(keys, contents)
        path = "/tmp/alpha_vantage_apple_data.parquet"
        df.to_parquet(path)
        return path

    @task()
    def _data_quality_check(path):
        df = pd.read_parquet(path)
        validate_trade_data(df)
        return path

    @task()
    def _store_data(path):
        snow_hook = SnowflakeHook(snowflake_conn_id="snowflake_con")
        conn = snow_hook.get_conn()
        df = pd.read_parquet(path)

        write_pandas(
            conn=conn,
            df=df,
            table_name="ALPHA_VANTAGE_APPLE_STAGING",
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )
        merge_sql = """
            MERGE INTO ALPHA_VANTAGE_APPLE AS target
            USING ALPHA_VANTAGE_APPLE_STAGING AS source
            ON target.date = source.date
            WHEN MATCHED THEN UPDATE SET
                target.open = source.open,
                target.close = source.close,
                target.high = source.high,
                target.low = source.low,
                target.volume = source.volume,
                target.daily_pct_change = source.daily_pct_change
            WHEN NOT MATCHED THEN INSERT (date, open, close, high, low, volume, daily_pct_change)
                VALUES (source.date, source.open, source.close, source.high, source.low, source.volume, source.daily_pct_change)
        """

        cursor = conn.cursor()
        try:
            cursor.execute(merge_sql)
            result = cursor.fetchone()
            print(f"Merge result: {result}")  # (rows_inserted, rows_updated)
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    process_data = _process_data(
        prepared.map(lambda p: {"date": p["date"]}), read_s3_data.output
    )
    data_quality_check = _data_quality_check(process_data)
    store_user = _store_data(data_quality_check)

    (
        [create_table, create_bucket]
        >> is_api_available
        >> extract_data
        >> prepared
        >> save_to_s3
        >> is_raw_data_save
        >> read_s3_data
        >> process_data
        >> data_quality_check
        >> store_user
    )


alpha_vantage_func()
