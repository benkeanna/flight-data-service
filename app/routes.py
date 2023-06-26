from __future__ import annotations

from flask import Response, request, current_app


def info() -> Response:
    result = current_app.repository.get_info()
    return Response(
        result.to_json(orient='records'),
        mimetype='application/json')


def aircrafts_models() -> Response:
    result = current_app.repository.get_aircraft_models()
    return Response(
        result.to_json(orient='records'),
        mimetype='application/json')


def aircrafts_filtered() -> Response:
    manufacturer = request.args.get('manufacturer')
    model = request.args.get('model')
    result = current_app.repository.get_aircrafts_filtered(manufacturer, model)
    return Response(
        result.to_json(orient='records'),
        mimetype='application/json')


def aircrafts_report() -> Response:
    result = current_app.repository.get_report()
    return Response(
        result.to_json(orient='records'),
        mimetype='application/json')


def aircrafts_report_pivot() -> Response:
    result = current_app.repository.get_report_pivot()
    return Response(
        result.to_json(orient='records'),
        mimetype='application/json')


def data_by_sql_string() -> Response:
    sql_string = request.json['sql']
    if not sql_string:
        return Response(
            "SQL not provided",
            status=400,
        )
    result = current_app.repository.get_data_by_sql_string(sql_string)
    return Response(
        result.to_json(orient='records'),
        mimetype='application/json')
