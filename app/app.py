import os

from flask import Flask, Response, request
from flask_caching import Cache
from pyarrow import parquet as pq

import services

data_dir = os.path.join(os.path.dirname(__file__), 'data')
filenames = os.listdir(data_dir)

app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})


def load():
    for filename in filenames:
        if not cache.get(filename):
            filepath = os.path.join(data_dir, filename)
            data = pq.read_table(filepath)
            cache.set(filename, data, timeout=86400)


def get_parquet_data():
    pd_data = {}
    for filename in filenames:
        data = cache.get(filename)
        if not data:
            load()
            data = cache.get(filename)
        pd_data[filename] = data
    return pd_data


@app.route('/info/')
def info():
    pd_data = get_parquet_data()
    pd_info = services.get_info(pd_data)
    return Response(
       pd_info.to_json(orient='index'),
       mimetype='application/json')


@app.route('/aircrafts/models/')
def aircrafts_models():
    pd_data = get_parquet_data()
    pd_info = services.get_aircraft_models(pd_data)
    return Response(
       pd_info.to_json(orient='index'),
       mimetype='application/json')


@app.route('/aircrafts/')
def aircrafts_filtered():
    manufacturer = request.args.get('manufacturer')
    model = request.args.get('model')
    pd_data = get_parquet_data()
    pd_info = services.get_aircrafts_filtered(pd_data, manufacturer, model)
    return Response(
       pd_info.to_json(orient='index'),
       mimetype='application/json')


@app.route('/aircrafts/reports/')
def aircrafts_report():
    pd_data = get_parquet_data()
    pd_info = services.get_report(pd_data)
    return Response(
       pd_info.to_json(orient='index'),
       mimetype='application/json')


@app.route('/aircrafts/reports/pivot/')
def aircrafts_report_pivot():
    pd_data = get_parquet_data()
    pd_info = services.get_report_pivot(pd_data)
    return Response(
       pd_info.to_json(orient='index'),
       mimetype='application/json')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

