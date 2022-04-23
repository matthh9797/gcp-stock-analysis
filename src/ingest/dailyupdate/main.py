import os
import logging
from flask import Flask
from flask import request, escape
from ingest_goog import ingest, next_date

app = Flask(__name__)


# App will only respond to POST requests (i.e. the json payload of arguements we invoke it with)
@app.route("/", methods=["POST"])
# Function will be run when app is invoked
def ingest_goog(request):
    try:
        logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.INFO)
        # Get json payload from POST request
        json = request.get_json(
            force=True
        )  # https://stackoverflow.com/questions/53216177/http-triggering-cloud-function-with-cloud-scheduler/60615210#60615210

        bucket = escape(json["bucket"])  # required
        date = escape(json["date"]) if "date" in json else None
        full_refresh = escape(json["full_refresh"]) if "full_refresh" in json else False

        if date is None:
            date = next_date(bucket)
        logging.debug("Ingesting date={}".format(date))
        try:
            tableref, numrows = ingest(date, bucket, full_refresh=full_refresh)
            ok = "Success ... ingested {} rows to {}".format(numrows, tableref)
            logging.info(ok)
            return ok
        except TypeError:
            pass
    except Exception as e:
        logging.exception("Failed to ingest ... try again later?")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
