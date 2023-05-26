import threading
from flask import Flask
from queue import Queue
from werkzeug.serving import ThreadedWSGIServer
from elasticsearch import Elasticsearch
import pyarrow.parquet as pq
import pandas as pd

app = Flask(__name__)
q = Queue()
es = Elasticsearch(['http://localhost:9200'])


@app.route('/api/<path>')
def get_example(path):
    q.put('C:/Users/LapStore/Downloads/' + path)
    return path


def receive():
    while True:
        path = q.get(block=True)
        parquet_table = pq.read_table(path)
        data_frame = parquet_table.to_pandas()
        documents = data_frame.to_dict(orient='records')
        bulk_data = []
        for doc in documents:
            print(doc)
            bulk_data.append({'index': {'_index': 'python-index'}})
            bulk_data.append(doc)
        es.bulk(index='python-index', operations=bulk_data)


if __name__ == '__main__':
    server = ThreadedWSGIServer('localhost', port=1200, app=app)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.start()
    receive()
