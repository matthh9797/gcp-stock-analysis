import os
import logging
import yfinance as yf
import tempfile  # Used to create temporary directories/files
import shutil

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
    except KeyError:
        s = "{} is not a trading day".format(date)
        logging.info(s)
        print(s)
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
        file = download(date, "repository", full_refresh=full_refresh)
        if file is not None:
            gcsloc = "stocks/raw/goog/{}.csv".format(date)
            gcsloc = upload(file, bucket, gcsloc)
            return bqload(gcsloc, full_refresh=full_refresh)

    finally:
        logging.debug("Cleaning up by removing {}".format(tempdir))
        shutil.rmtree(tempdir)


def next_date():
    """Get next day from bucket"""
    pass

