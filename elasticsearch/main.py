import threading
from flask import Flask, request
from queue import Queue
from werkzeug.serving import ThreadedWSGIServer
from elasticsearch import Elasticsearch
import pyarrow.parquet as pq

app = Flask(__name__)
q = Queue()
es = Elasticsearch(['http://elasticsearch-kibana-service:9200'])

@app.route('/api', methods=['POST'])
def get_example():
    print(request)
    path = request.json.get('path')
    q.put(path)
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
    server = ThreadedWSGIServer('0.0.0.0', port = 5000, app=app)
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.start()
    receive()
