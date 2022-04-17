import argparse
import os
import logging
from yaml import parse
import yfinance as yf
import tempfile  # Used to create temporary directories/files
import shutil
import datetime

from google.cloud import storage
from google.cloud.storage import Blob
from google.cloud import bigquery


def download(date: str, destdir: str, full_refresh: bool):
    """
    Downloads Google ticker market close data and returns local file name
    date e.g. '2019-04-05'
    """
    logging.info("Requesting GOOG data for {}".format(date))
    try:
        goog = yf.Ticker("GOOG")
        history = goog.history(period="max")
        if full_refresh is True:
            history = history.loc[:date].reset_index()
        else:
            history = history.loc[[date]].reset_index()
        filename = os.path.join(destdir, "{}".format(date))
        history.to_csv(filename, index=False)
        logging.debug("{} saved".format(filename))
        return filename
    # If day is not a trading day, the date will not exist
    except KeyError:
        s = "{} is not a trading day".format(date)
        logging.info(s)
        return None


def upload(csvfile, bucketname, blobname):
    """
    Uploads the csv file into the bucket with given blobname
    """
    client = storage.Client()
    bucket = client.get_bucket(bucketname)
    logging.info(bucket)
    blob = Blob(blobname, bucket)
    logging.debug("Uploading {} ...".format(csvfile))
    blob.upload_from_filename(csvfile)
    gcslocation = "gs://{}/{}".format(bucketname, blobname)
    logging.info("Uploaded {} ...".format(gcslocation))
    return gcslocation


def bqload(gcsfile: str, full_refresh: bool):
    """
    Loads the csv file in GCS to BigQuery, replacing data in the
    current partition
    """
    client = bigquery.Client()
    table_ref = client.dataset("gcp_stock_analysis").table("stock_price")
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = "CSV"
    if full_refresh is True:
        job_config.write_disposition = "WRITE_TRUNCATE"
    else:
        job_config.write_disposition = "WRITE_APPEND"
    job_config.ignore_unknown_values = True
    job_config.skip_leading_rows = 1
    # job_config.autodetect = True
    job_config.schema = [
        bigquery.SchemaField(
            col_and_type.split(":")[0], col_and_type.split(":")[1]
        )  # , mode='required')
        for col_and_type in [
            "Date:DATE",
            "Open:FLOAT64",
            "High:FLOAT64",
            "Low:FLOAT64",
            "Close:FLOAT64",
            "Volume:NUMERIC",
            "Dividend:NUMERIC",
            "Stock_Splits:NUMERIC",
        ]
    ]
    load_job = client.load_table_from_uri(gcsfile, table_ref, job_config=job_config)
    load_job.result()

    if load_job.state != "DONE":
        raise load_job.exception()

    return table_ref, load_job.output_rows


def ingest(date: str, bucket: str, full_refresh=False):
    """Ingest data from yahoo finance api into BigQuery
    return table, numrows on success
    """
    tempdir = tempfile.mkdtemp(prefix="ingest_stocks")
    try:
        file = download(date, tempdir, full_refresh=full_refresh)
        if file is None:
            pass
        else:
            gcsloc = "stocks/raw/goog/{}.csv".format(date)
            gcsloc = upload(file, bucket, gcsloc)
            return bqload(gcsloc, full_refresh=full_refresh)
    finally:
        logging.debug("Cleaning up by removing {}".format(tempdir))
        shutil.rmtree(tempdir)


def compute_next_month(date):
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    dt = dt + datetime.timedelta(1)
    logging.debug("The next date is {}".format(dt))
    return dt.strftime("%Y-%m-%d")


def next_date(bucketname: str):
    """ 
    Finds which dates exist in GCP and returns the next date to download
    """
    client = storage.Client()
    bucket = client.get_bucket(bucketname)
    blobs = list(bucket.list_blobs(prefix="stocks/raw/goog/"))
    files = [blob.name for blob in blobs if "csv" in blob.name]
    lastfile = os.path.basename(files[-1])
    logging.debug("The latest file on GCS is {}".format(lastfile))
    date = os.path.splitext(lastfile)[0]
    return compute_next_month(date)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Ingest ticker data from yahoo finance in BigQuery"
    )
    parser.add_argument("--bucket", help="GCS bucket to upload to", required=True)
    parser.add_argument(
        "--date",
        help="Example: 2020-01-01. If not provided, defaults to getting next month",
    )
    parser.add_argument(
        "--full_refresh", help="Fully refresh all data in table. Defaults to False",
    )
    parser.add_argument(
        "--debug",
        dest="debug",
        action="store_true",
        help="Specify if you want debug messages",
    )

    try:
        args = parser.parse_args()
        if args.debug:
            logging.basicConfig(
                format="%(levelname)s: %(message)s", level=logging.DEBUG
            )
        else:
            logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)

        if args.full_refresh is None:
            full_refresh = False
        else:
            full_refresh = args.full_refresh

        if args.date is None:
            # _ to avoid conflicts with python keywords
            date_ = next_date(args.bucket)
        else:
            date_ = args.date

            logging.debug("Ingesting date={}".format(date_))
            try:
                tableref, numrows = ingest(
                    date_, args.bucket, full_refresh=full_refresh
                )
                logging.info(
                    "Success ... ingested {} rows to {}".format(numrows, tableref)
                )
            except TypeError:
                pass
    except Exception as e:
        logging.exception("Try again later?")
