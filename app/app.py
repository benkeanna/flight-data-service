from flask import Flask, Response, request
from extensions import cache

import services

app = Flask(__name__)
cache.init_app(app, config={'CACHE_TYPE': 'simple'})


@app.route('/info/')
def info():
    pd_info = services.get_info()
    return Response(
       pd_info.to_json(orient='index'),
       mimetype='application/json')


@app.route('/aircrafts/models/')
def aircrafts_models():
    pd_info = services.get_aircraft_models()
    return Response(
       pd_info.to_json(orient='index'),
       mimetype='application/json')


@app.route('/aircrafts/')
def aircrafts_filtered():
    manufacturer = request.args.get('manufacturer')
    model = request.args.get('model')
    pd_info = services.get_aircrafts_filtered(manufacturer, model)
    return Response(
       pd_info.to_json(orient='index'),
       mimetype='application/json')


@app.route('/aircrafts/reports/')
def aircrafts_report():
    pd_info = services.get_report()
    return Response(
       pd_info.to_json(orient='index'),
       mimetype='application/json')


@app.route('/aircrafts/reports/pivot/')
def aircrafts_report_pivot():
    pd_info = services.get_report_pivot()
    return Response(
       pd_info.to_json(orient='index'),
       mimetype='application/json')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

