import os
import logging
import yfinance as yf

from google.cloud import storage
from google.cloud.storage import Blob
from google.cloud import bigquery


def download(date: str, destdir: str):
    """
    Downloads Google ticker market close data and returns local file name
    date e.g. '2019-04-05'
    """
    logging.info("Requesting GOOG data for {}".format(date))
    try:
        goog = yf.Ticker("GOOG")
        history = goog.history(period="max")
        snapshot = history.loc[[date]].reset_index()
        filename = os.path.join(destdir, "{}".format(date))
        snapshot.to_csv(filename)
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


def bqload(gcsfile, date):
    """
    Loads the csv file in GCS to BigQuery, replacing data in the
    current partition
    """
    client = bigquery.Client()
    table_ref = client.dataset("gcp_stock_analysis")


def ingest(date, bucket):
    """Ingest data into bucket"""


def next_date():
    """Get next day from bucket"""
    pass

