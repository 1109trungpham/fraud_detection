import json
import datetime
from pyspark.sql.streaming import StreamingQueryListener

class CustomListener(StreamingQueryListener):

    def onQueryStarted(self, event):
        print(f"[START] Query {event.name} ({event.id}) - {datetime.datetime.now()}")

    def onQueryProgress(self, event):
        progress = json.loads(event.progress.json)
        batch_id = progress["batchId"]
        num_rows = progress["numInputRows"]
        print(f"[PROGRESS] Batch {batch_id} - Rows: {num_rows}")

    def onQueryTerminated(self, event):
        print(f"[STOP] Query {event.name} - {datetime.datetime.now()}")


def attach_listener(spark):
    listener = CustomListener()
    spark.streams.addListener(listener)
    print("[INFO] CustomListener attached.")
