import gspread as gs
import pandas as pd
import psycopg2
from os import environ, getenv
from dotenv import load_dotenv
import logging as log
from datetime import datetime
from sys import exit
from ast import literal_eval


# ENVs
load_dotenv()

PIPELINE_NAME = getenv('PIPELINE_NAME', 'UNNAMED')
PIPELINE_LOG_LEVEL = getenv('PIPELINE_LOG_LEVEL', 'info')

LOG_LEVELS = {
    'CRITICAL': 50,
    'ERROR': 40,
    'WARNING': 30,
    'INFO': 20,
    'DEBUG': 10,
    'NOTSET': 0
}


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
        s3_start = datetime.now()
        # stream the dataframe to s3 bucket
        df.to_csv(f'{s3_uri}', index=False, storage_options={"key": s3_key, "secret": s3_secret})
        log.info(f'Data has been uploaded to `{s3_uri}`. Time taken {datetime.now() - s3_start}')
    except Exception as e:
        log.exception(f'CSV file upload to AWS S3 has failed. Time taken {datetime.now() - s3_start}')
        exit(1)


def google_sheet_to_s3(
        google_sheet_id: str,
        google_service_account_credentials: dict,
        s3_uri: str,
        s3_key: str,
        s3_secret: str) -> pd.DataFrame:
    """Read google sheet data and load every worksheet into a single pandas dataframe.

    WARNING: To be used with spreadsheets with all worksheets consisting only with `=GOOGLEFINANCE(...)` data!

    Args:
        google_sheet_id (str): Google sheet ID
        google_service_account_credentials (dict): TODO

    Returns:
        pd.DataFrame: Pandas dataframe containing data from every worksheet in read google sheet
    """
    log.debug(f'Connecting to google sheet with id "{google_sheet_id}"')
    log.debug(f'Google API credentials file used "{google_service_account_credentials}"')
    try:
        gs_start = datetime.now()
        gc = gs.service_account_from_dict(google_service_account_credentials)
        sh = gc.open_by_key(google_sheet_id)
        log.info(f'Done reading google sheet. Time taken {datetime.now() - gs_start}')
    except Exception as e:
        log.exception(f'Failed to read google sheet with id "{google_sheet_id}". Time taken {datetime.now() - gs_start}')
        exit(1)
    for worksheet in sh.worksheets():
        wh_start = datetime.now()
        log.debug(f'Processing worksheet with name "{worksheet.title}"')
        try:
            # read worksheet by id into pandas df
            ws = sh.get_worksheet_by_id(worksheet.id)
            df = pd.DataFrame(ws.get_all_records())
            # get the currency from, to labels from worksheet name
            currency_labels = worksheet.title.split('2')
            s3_filename = f'{currency_labels[0]}_{currency_labels[1]}.csv'
            df = currency_exchange_sheet_post_processing(df, currency_labels)
            # Strem the CSV to S3 bucket
            stream_dataframe_to_s3(
                df,
                s3_uri + s3_filename,
                s3_key,
                s3_secret
            )
            log.debug(f'Processing worksheet with name "{worksheet.title}". Time taken {datetime.now() - wh_start}')
        except Exception as e:
            log.exception(f'Failed processing worksheet with name "{worksheet.title}". Time taken {datetime.now() - wh_start}')
            exit(1)
    log.info(f'Done processing google sheet. Time taken {datetime.now() - gs_start}"')


def load_csv_into_redshift(
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
        s3_uri (str): AWS S3 directory URI
        s3_key (str): AWS S3 access key
        s3_secret (str): AWS S3 access secret
    """
    rs_start = datetime.now()
    try:
        log.debug('Connecting to AWS Redshift')
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
                log.debug('Truncating AWS Redshift stage table')
                cur.execute(truncate_command)
                log.debug('Truncating AWS Redshift stage table has been successful')

            with conn.cursor() as cur:
                log.debug('Loading data into AWS Redshift')
                cur.execute(copy_command)
                # get number of inserted records
                cur.execute("SELECT pg_last_copy_count()")
                log.debug('Loading data into AWS Redshift has been successful. Affected rows: {}'.format(cur.fetchone()[0]))

            # TODO: Should be done by dbt!
            with conn.cursor() as cur:
                log.debug('Running upsert on target table')
                cur.execute(upsert_command)
                log.debug(f'Running upsert on target table has been successful. Affected rows: {cur.rowcount}')
        log.info(f'Successully run data refresh on AWS Redshift. Time taken {datetime.now() - rs_start}')
    except Exception as e:
        log.exception(
            f'Failed running COPY ... CSV ... on AWS Redshift. Time taken {datetime.now() - rs_start}'
        )

    finally:
        conn.close()


# Only applicable to currency exchange spreadsheet
def currency_exchange_sheet_post_processing(
        df: pd.DataFrame,
        currency_labels: list[str]) -> pd.DataFrame:
    """Performs currency exchange worksheet specific data cleaning

    Args:
        df (pd.DataFrame): Initial worksheet dataframe
        worksheet_title (list[str]): List of two names - [0] Currency from, [1] Currency to

    Returns:
        pd.DataFrame: Post data cleanse worksheet dataframe
    """
    # column names must be lowercase
    df.columns = [col.lower() for col in df.columns]
    # convert datetime string to datetime dtype
    df['date'] = pd.to_datetime(df['date'])
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


def pipeline():
    """Pipeline execution

    - Google sheet's worksheets to CSVs in S3 bucket
        - Retrieves data from specified spreadsheet (env. variable)
        - Iterates over every worksheet and cleans up the data
        - Uploads each worksheet to single CSV file on AWS S3 bucket (env. variables). Worksheet name must be <currency_from>2<currency_to>
    - Loads data into Redshift
        TODO: We want to get rid of "staging" notion
        - Truncates {PIPELINE_AWS_REDSHIFT_TABLE}_stage table
        - Runs COPY ... CSV .. job to load data into {PIPELINE_AWS_REDSHIFT_TABLE}_stage table
        - Upserts missing data into {PIPELINE_AWS_REDSHIFT_TABLE} by [date, currency_from, currency_to] attributes, when comparing with staging
    """

    log.basicConfig(
        format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
        datefmt='%Y-%m-%d,%H:%M:%S',
        level=LOG_LEVELS[PIPELINE_LOG_LEVEL.upper()],
        force=True
    )

    # Check if mandatory envs are found
    try:
        PIPELINE_AWS_S3_URI = environ['PIPELINE_AWS_S3_URI']
        PIPELINE_AWS_ACCESS_KEY_ID = environ['PIPELINE_AWS_ACCESS_KEY_ID']
        PIPELINE_AWS_SECRET_ACCESS_KEY = environ['PIPELINE_AWS_SECRET_ACCESS_KEY']

        PIPELINE_AWS_REDSHIFT_DSN = environ['PIPELINE_AWS_REDSHIFT_DSN']
        PIPELINE_AWS_REDSHIFT_TABLE = environ['PIPELINE_AWS_REDSHIFT_TABLE']

        PIPELINE_GOOGLE_SHEET_ID = environ['PIPELINE_GOOGLE_SHEET_ID']
        PIPELINE_GOOGLE_SERVICE_ACCOUNT = environ['PIPELINE_GOOGLE_SERVICE_ACCOUNT']
    except KeyError:
        log.exception('Missing environmental variable!')
        exit(1)

    pl_start = datetime.now()
    log.info(f'Pipeline "{PIPELINE_NAME}" started as standalone')
    try:

        # 1. Get data from google sheets
        google_sheet_to_s3(
            google_sheet_id=PIPELINE_GOOGLE_SHEET_ID,
            google_service_account_credentials=literal_eval(PIPELINE_GOOGLE_SERVICE_ACCOUNT),
            s3_uri=PIPELINE_AWS_S3_URI,
            s3_key=PIPELINE_AWS_ACCESS_KEY_ID,
            s3_secret=PIPELINE_AWS_SECRET_ACCESS_KEY
        )

        # 2. Load CSV into AWS Redshift from S3
        load_csv_into_redshift(
            redshift_dsn=PIPELINE_AWS_REDSHIFT_DSN,
            redshift_table=PIPELINE_AWS_REDSHIFT_TABLE,
            s3_uri=PIPELINE_AWS_S3_URI,
            s3_key=PIPELINE_AWS_ACCESS_KEY_ID,
            s3_secret=PIPELINE_AWS_SECRET_ACCESS_KEY
        )

        log.info(f'Pipeline finished successfully. Time taken {datetime.now() - pl_start}')
    except Exception as e:
        log.exception(f'Unhandled error occured. Time taken {datetime.now() - pl_start}')


# For AWS Lambda only
def handler(event, context):
    pipeline()


if __name__ == '__main__':
    pipeline()
