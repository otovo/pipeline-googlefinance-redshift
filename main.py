from typing import final
import gspread as gs
import pandas as pd
import psycopg2
from os import getenv
from dotenv import load_dotenv
import logging as log
import time
from sys import exit

# ENVs
load_dotenv()

AWS_S3_URI = getenv('AWS_S3_URI')
AWS_ACCESS_KEY_ID = getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = getenv('AWS_SECRET_ACCESS_KEY')

AWS_REDSHIFT_DNS = getenv('AWS_REDSHIFT_DNS')
AWS_REDSHIFT_TABLE = getenv('AWS_REDSHIFT_TABLE')

PIPELINE_NAME = getenv('PIPELINE_NAME', 'UNNAMED')
PIPELINE_LOG_LEVEL = getenv('PIPELINE_LOG_LEVEL', 'info')

GOOGLE_SHEET_ID = getenv('GOOGLE_SHEET_ID')
GOOGLE_API_CREDENTIALS_FILE = getenv('GOOGLE_API_CREDENTIALS_FILE')


def stream_dataframe_to_s3(
        df: pd.DataFrame,
        s3_uri: str,
        s3_key: str,
        s3_secret: str) -> None:
    """Uploads pandas dataframe to a CSV file specified by the S3 URI

    Args:
        df (pd.DataFrame): Pandas dataframe with data to be written to the CSV
        s3_uri (str): AWS S3 file key for the CSV file
        s3_key (str): AWS S3 access key
        s3_secret (str): AWS S3 access secret
    """
    try:
        log.debug(f'Uploading the data to "{s3_uri}"')
        s3_start = time.process_time()
        # stream the dataframe to s3 bucket
        df.to_csv(f'{s3_uri}', index=False, storage_options={"key": s3_key, "secret": s3_secret})
        log.info(f'Data has been uploaded to "{s3_uri}". Time taken {time.process_time() - s3_start} seconds')
    except Exception as e:
        log.exception(f'CSV file upload to AWS S3 has failed. Time taken {time.process_time() - s3_start} seconds')
        exit(1)


def google_sheet_to_df(
        google_sheet_id: str,
        google_api_credentials_file: str) -> pd.DataFrame:
    """Read google sheet data and load every worksheet into a single pandas dataframe.

    WARNING: To be used with spreadsheets with all worksheets consisting only with `=GOOGLEFINANCE(...)` data!

    Args:
        google_sheet_id (str): Google sheet ID
        google_api_credentials_file (str): Full path to google's service account credential file

    Returns:
        pd.DataFrame: Pandas dataframe containing data from every worksheet in read google sheet
    """
    data_df = pd.DataFrame()
    log.debug(f'Connecting to google sheet with id "{google_sheet_id}"')
    log.debug(f'Google API credentials file used "{google_api_credentials_file}"')
    try:
        gs_start = time.process_time()
        gc = gs.service_account(google_api_credentials_file)
        sh = gc.open_by_key(google_sheet_id)
        log.info(f'Done reading google sheet. Time taken {time.process_time() - gs_start} seconds')
    except Exception as e:
        log.exception(f'Failed to read google sheet with id "{google_sheet_id}". Time taken {time.process_time() - gs_start} seconds')
        exit(1)
    # combine all sheets into single dataframe
    for worksheet in sh.worksheets():
        try:
            wh_start = time.process_time()
            log.debug(f'Processing worksheet with name "{worksheet.title}"')
            # read worksheet by id into pandas df
            ws = sh.get_worksheet_by_id(worksheet.id)
            df = pd.DataFrame(ws.get_all_records())
            df = currency_exchange_sheet_post_processing(df, worksheet.title)
            # append the dataframe to the main dataframe
            data_df = pd.concat([data_df, df], ignore_index=True)
            log.debug(f'Processing worksheet with name "{worksheet.title}". Time taken {time.process_time() - wh_start} seconds')
        except Exception as e:
            log.exception(f'Failed processing worksheet with name "{worksheet.title}". Time taken {time.process_time() - wh_start}')
            exit(1)
    log.info(f'Done processing google sheet. Time taken {time.process_time() - gs_start} seconds"')
    return data_df


def redshift_copy_csv(
        redshift_dsn: str,
        redshift_table: str,
        s3_uri: str,
        s3_key: str,
        s3_secret: str) -> None:
    """Runs

    COPY {redshift_table} (date, currency_from, currency_to, close)
        FROM '{s3_uri}'
        CREDENTIALS 'aws_access_key_id={s3_key};aws_secret_access_key={s3_secret}'
        IGNOREHEADER 1
        DELIMITER ','

    Args:
        redshift_dsn (str): AWS Redshift connection DSN
        redshift_table (str): AWS Redshift target table
        s3_uri (str): AWS S3 file or directory URI
        s3_key (str): AWS S3 access key
        s3_secret (str): AWS S3 access secret
    """
    rs_start = time.process_time()
    try:
        log.debug(f'Connecting to AWS Redshift')
        conn = psycopg2.connect(redshift_dsn)
        # Truncates the stage table
        # TODO: We should assume there is no stage table for this job!
        truncate_command = f"TRUNCATE {redshift_table}_stage"
        # Loads data in stage table
        # TODO: We should assume there is no stage table for this job!
        copy_command = f"""
        COPY {redshift_table}_stage (date, currency_from, currency_to, close)
            FROM '{s3_uri}'
            CREDENTIALS 'aws_access_key_id={s3_key};aws_secret_access_key={s3_secret}'
            IGNOREHEADER 1
            DELIMITER ','
        """
        # Move data from stage to target table
        # TODO: Should be removed. This should be left to a dbt job!
        upsert_command = f"""
        INSERT INTO {redshift_table}
        SELECT
            stage.*
        FROM {redshift_table} target
            RIGHT JOIN {redshift_table}_stage stage USING (date, currency_from, currency_to)
        WHERE target.date IS NULL
        """
        with conn:
            with conn.cursor() as cur:
                log.debug(f'Truncating AWS Redshift stage table')
                cur.execute(truncate_command)
                log.debug(f'Truncating AWS Redshift stage table has been successful')

            with conn.cursor() as cur:
                log.debug(f'Loading data into AWS Redshift')
                cur.execute(copy_command)
                # get number of inserted records
                cur.execute("SELECT pg_last_copy_count()")
                log.debug('Loading data into AWS Redshift has been successful. Affected rows: {}'.format(cur.fetchone()[0]))

            # TODO: Should be done by dbt!
            with conn.cursor() as cur:
                log.debug(f'Running upsert on target table')
                cur.execute(upsert_command)
                log.debug(f'Running upsert on target table has been successful. Affected rows: {cur.rowcount}')
        log.info(f'Successully run data refresh on AWS Redshift. Time taken {time.process_time() - rs_start} seconds')
    except Exception as e:
        log.exception(f'Failed running COPY ... CSV ... on AWS Redshift. Time taken {time.process_time() - rs_start} seconds')
    finally:
        conn.close()


# Only applicable to currency exchange spreadsheet
def currency_exchange_sheet_post_processing(
        df: pd.DataFrame,
        worksheet_title: str) -> pd.DataFrame:
    """Performs currency exchange worksheet specific data cleaning

    Args:
        df (pd.DataFrame): Initial worksheet dataframe
        worksheet_title (str): Worksheet title to exctact from, to currency labels. Must be delimited with '2' character

    Returns:
        pd.DataFrame: Post data cleanse worksheet dataframe
    """
    # column names must be lowercase
    df.columns = [col.lower() for col in df.columns]
    # convert datetime string to datetime dtype
    df['date'] = pd.to_datetime(df['date'])
    # get the currency from, to labels from worksheet name
    currency_labels = worksheet_title.split('2')
    # add the labels
    df = add_currency_labels(df, currency_labels[0], currency_labels[1])
    return df


# Only applicable to currency exchange spreadsheet
def add_currency_labels(df: pd.DataFrame, from_value: str, to_value: str) -> pd.DataFrame:
    """Adds 'from' and 'to' columns as well as their values.
    Then rearranges the dataframe columns to ['date', 'currency_from', 'currency_to', 'close']

    Args:
        df (pd.DataFrame): Initial dataframe for the labels to be added to
        from_value (str): 'from' column value
        to_value (str): 'to' column value

    Returns:
        pd.DataFrame: [description]
    """
    # append currency labels
    df['currency_from'] = from_value
    df['currency_to'] = to_value
    return df[['date', 'currency_from', 'currency_to', 'close']]


def main():
    """Pipeline execution

    - Read specified google sheet
        - Retrieves data from specified spreadsheet (env. variable)
        - Iterates over every worksheet and cleans up the data
    - Uploads combined data to single CSV file on AWS S3 bucket (env. variables)
    - Runs COPY ... CSV .. job
    """

    # 1. Get data from google sheets
    # - Connect to google sheets using google's drive API
    # - Download data from every sheet
    # - Data cleanup
    # - Return concatinated dataframe containing data from every worksheet
    df = google_sheet_to_df(
        google_sheet_id=GOOGLE_SHEET_ID,
        google_api_credentials_file=GOOGLE_API_CREDENTIALS_FILE
    )

    # 2. Upload dataframe to S3 bucket
    stream_dataframe_to_s3(
        df,
        s3_uri=AWS_S3_URI,
        s3_key=AWS_ACCESS_KEY_ID,
        s3_secret=AWS_SECRET_ACCESS_KEY
    )

    # 3. Load CSV into AWS Redshift from S3
    redshift_copy_csv(
        redshift_dsn=AWS_REDSHIFT_DNS,
        redshift_table=AWS_REDSHIFT_TABLE,
        s3_uri=AWS_S3_URI,
        s3_key=AWS_ACCESS_KEY_ID,
        s3_secret=AWS_SECRET_ACCESS_KEY
    )


if __name__ == '__main__':
    log_levels = {
        'CRITICAL': 50,
        'ERROR': 40,
        'WARNING': 30,
        'INFO': 20,
        'DEBUG': 10,
        'NOTSET': 0
    }
    log.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
        datefmt='%Y-%m-%d,%H:%M:%S',
        level=log_levels[PIPELINE_LOG_LEVEL.upper()]
    )

    pl_start = time.process_time()

    log.info(f'Pipeline "{PIPELINE_NAME}" started as standalone')
    try:
        main()
    except Exception as e:
        log.exception(f'Unhandled error occured. Time taken {time.process_time() - pl_start} seconds')
    log.info(f'Pipeline finished successfully. Time taken {time.process_time() - pl_start} seconds')
