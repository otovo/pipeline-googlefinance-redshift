# pipeline-googlefinance-redshift

## About The Project

Quick and dirty python pipeline designed to read currency exchange rates returned by `=GOOGLEFINANCE` function and have them sunk into the Redshift table.

The pipeline will not rewrite the data in the target table, but instead append *the missing* tuples only! This is done because past currency exchange rates are not supposed to change, however we may have missing data for one reson or another.

***Pipeline will fulfill following tasks***

* Read every worksheet in specified ***Google sheets*** doc
* Load the data into single ***Pandas*** dataframe
* Stream the dataframe to a ***CSV*** file in ***AWS S3 bucket***
* Truncate the staging table
* Trigger COPY ... CSV ... FROM S3 on a ***AWS Redshift cluster***
* Append the missing rows to the target table

### From Google sheet

![google_worksheet](/static/img/google_worksheet.png)

### To Redshift table

#### SQL

```sql
SELECT
    date :: DATE,
    currency_from,
    currency_to,
    close
FROM currency_exchange.rates
WHERE currency_from IN ('eur', 'sek')
  AND currency_to = 'nok'
ORDER BY 1 DESC
LIMIT 5
```

#### Result

|    date    | currency_from | currency_to |    close    |
|------------|---------------|-------------|-------------|
| 2021-09-11 | sek           | nok         | 1.008823137 |
| 2021-09-11 | eur           | nok         |    10.24494 |
| 2021-09-10 | sek           | nok         |      1.0049 |
| 2021-09-10 | eur           | nok         |    10.24494 |
| 2021-09-09 | sek           | nok         |     1.00595 |

### Built With

Python 3. See [requirements.txt](requirements.txt) for more info on packages used.

## Getting Started

To get a local copy up and running follow these simple steps.

### Prerequisites

* Python 3.8 or newer
* S3 access
* Google sheets access
* AWS Redshift cluster

### Installation

1. Clone the repo

   ```sh
   git clone https://github.com/otovo/pipeline-googlefinance-redshift.git
   cd pipeline-googlefinance-redshift
   ```

2. Create and activate your virtual environment

   ```sh
   python3 -m pip install virtualenv && python3 -m venv venv
   source venv/bin/activate
   ```

3. Install required packages

    ```sh
    python -m pip install -r requirements.txt
    ```

4. Create and update the `app/.env` file.

    ```sh
    cp app/.env_example app/.env
    $EDITOR app/.env
    ```

## Google's service account

In order to get the `service_account.json` file, one must follow [Create a service account](https://support.google.com/a/answer/7378726?hl=en) instructions. You will need need to place the contents of the file in the `GOOGLE_SERVICE_ACCOUNT` env var.

One can quickly one-line the contents of a json file with

```text
jq -c . < /path/to/service_account.json
```

### Accessing spreadsheet via the service account

For the service account to be able to access the google sheet, one must share the spreadsheet with the email specified in `service_account.json`. The email is the value for key `"client_email"` in the file.

## Read before running the pipeline

### Google Sheet

Every worksheet in the spreadsheet must only contain `=GOOGLEFINANCE`'s 'price' generated content.
In addition, the worksheet names must be of following pattern

```text
<sell>2<buy>
```

For example

```text
nok2eur
```

Which will then be represented in Redshift table as

| date | currency_from | currency_to | rate |
| --- | --- | --- | --- |
| ... | nok | eur | ... |

### AWS S3

One must be able to both read and write to the specified S3 bucket or "directory" within the bucket with the credentials provided as either host's env. vars or in `app/.env` file.

### Redshift

Before the pipeline can successfully run the Redshift task, one must make sure to have following privileges and database objects created in the schema you intend to use.

For the recipe, we are assuming following:

* ***AWS_REDSHIFT_TABLE*** is `currency_exchange.rates`
* ***AWS_REDSHIFT_DNS*** username is `currency_exchange_pipeline`

#### Recipe

```sql
CREATE SCHEMA IF NOT EXISTS currency_exchange;

-- Create the target table
DROP TABLE IF EXISTS currency_exchange.rates;
CREATE TABLE IF NOT EXISTS currency_exchange.rates
(
    "date"          TIMESTAMP,
    currency_from VARCHAR(3),
    currency_to   VARCHAR(3),
    close         FLOAT
);

-- Create the staging table. Name of it must be <TARGET TABLE>_stage
DROP TABLE IF EXISTS currency_exchange.rates_stage;
CREATE TABLE IF NOT EXISTS currency_exchange.rates_stage
(
    "date"          TIMESTAMP,
    currency_from VARCHAR(3),
    currency_to   VARCHAR(3),
    close         FLOAT
);

-- Privileges for the new objects and schema
GRANT USAGE ON SCHEMA currency_exchange TO currency_exchange_pipeline;
GRANT SELECT, INSERT ON TABLE currency_exchange.rates TO currency_exchange_pipeline;

-- TRUNCATE requires the user to be owner
ALTER TABLE currency_exchange.rates_stage
    OWNER TO currency_exchange_pipeline;
-- Since the connecting user is now owner of staging table,
-- no additional privileges are required
```

## Usage

When everything has been set up and configured (see above), running the pipeline is as simple as

```sh
python app/run.py
```

### Log examples

`PIPELINE_LOG_LEVEL = "info"`

```text
2021-09-11,22:01:45.017 INFO {main} [<module>] Pipeline "googlefinance-redshift" started as standalone
2021-09-11,22:01:45.018 INFO {main} [google_sheet_to_df] Done reading google sheet. Time taken 0.006516999999999662 seconds
2021-09-11,22:01:55.264 INFO {main} [google_sheet_to_df] Done processing google sheet. Time taken 0.7311729999999996 seconds"
2021-09-11,22:01:55.835 INFO {main} [stream_dataframe_to_s3] Data has been uploaded to S3. Time taken 0.15680499999999986 seconds
2021-09-11,22:01:58.172 INFO {main} [load_csv_into_redshift] Successully run data refresh on AWS Redshift. Time taken 0.008175999999999739 seconds
2021-09-11,22:01:58.173 INFO {main} [<module>] Pipeline finished successfully. Time taken 0.9001799999999998 seconds
```

## Roadmap

None whastover, however feel free to see the [open issues](https://github.com/otovo/pipeline-googlefinance-redshift/issues) for a list of proposed features (and known issues).

<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE` for more information.
