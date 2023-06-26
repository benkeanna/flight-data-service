from __future__ import annotations

from flask import Flask, current_app

import services
import routes


def create_app() -> Flask:
    app = Flask(__name__)

    app.config.from_prefixed_env()
    repository = services.faa_data_repository_factory(app.config['FAA_DATA_DIR_PATH'])
    with app.app_context():
        current_app.repository = repository

    app.add_url_rule('/info/', 'info', routes.info)
    app.add_url_rule('/aircrafts/models/', 'aircrafts_models', routes.aircrafts_models)
    app.add_url_rule('/aircrafts/', 'aircrafts_filtered', routes.aircrafts_filtered)
    app.add_url_rule('/aircrafts/reports/', 'aircrafts_report', routes.aircrafts_report)
    app.add_url_rule('/aircrafts/reports/pivot/', 'aircrafts_report_pivot', routes.aircrafts_report_pivot)
    app.add_url_rule('/sql/', 'data_by_sql_string', routes.data_by_sql_string, methods=['POST'])

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=5000, debug=True)