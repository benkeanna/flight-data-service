from flask import Flask, Response, request
from flask_caching import Cache

import services

app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})
repository = services.DataRepository(cache)


@app.route('/info/')
def info():
    pd_info = repository.get_info()
    return Response(
       pd_info.to_json(orient='records'),
       mimetype='application/json')


@app.route('/aircrafts/models/')
def aircrafts_models():
    pd_info = repository.get_aircraft_models()
    return Response(
       pd_info.to_json(orient='records'),
       mimetype='application/json')


@app.route('/aircrafts/')
def aircrafts_filtered():
    manufacturer = request.args.get('manufacturer')
    model = request.args.get('model')
    pd_info = repository.get_aircrafts_filtered(manufacturer, model)
    return Response(
       pd_info.to_json(orient='records'),
       mimetype='application/json')


@app.route('/aircrafts/reports/')
def aircrafts_report():
    pd_info = repository.get_report()
    return Response(
       pd_info.to_json(orient='records'),
       mimetype='application/json')


@app.route('/aircrafts/reports/pivot/')
def aircrafts_report_pivot():
    pd_info = repository.get_report_pivot()
    return Response(
       pd_info.to_json(orient='records'),
       mimetype='application/json')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

