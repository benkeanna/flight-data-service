import requests


def api_test():
    """API integration test."""
    info = requests.get('http://127.0.0.1:5000/info/')
    assert info.status_code == 200
    assert info.json() == {
        '0': {'data_file_name': 'aircraft.parquet',
              'column_list': ['id', 'tail_num', 'aircraft_serial', 'aircraft_model_code', 'aircraft_engine_code', 'year_built', 'aircraft_type_id', 'aircraft_engine_type_id', 'registrant_type_id', 'name', 'address1', 'address2', 'city', 'state', 'zip', 'region', 'county', 'country', 'certification', 'status_code', 'mode_s_code', 'fract_owner', 'last_action_date', 'cert_issue_date', 'air_worth_date'],
              'total_number_of_rows': 3599},
        '1': {'data_file_name': 'aircraft_models.parquet',
              'column_list': ['aircraft_model_code', 'manufacturer', 'model', 'aircraft_type_id', 'aircraft_engine_type_id', 'aircraft_category_id', 'amateur', 'engines', 'seats', 'weight', 'speed'],
              'total_number_of_rows': 60461},
        '2': {'data_file_name': 'airports.parquet',
              'column_list': ['id', 'code', 'site_number', 'fac_type', 'fac_use', 'faa_region', 'faa_dist', 'city', 'county', 'state', 'full_name', 'own_type', 'longitude', 'latitude', 'elevation', 'aero_cht', 'cbd_dist', 'cbd_dir', 'act_date', 'cert', 'fed_agree', 'cust_intl', 'c_ldg_rts', 'joint_use', 'mil_rts', 'cntl_twr', 'major'],
              'total_number_of_rows': 19793},
        '3': {'data_file_name': 'carriers.parquet',
              'column_list': ['code', 'name', 'nickname'],
              'total_number_of_rows': 21},
        '4': {'data_file_name': 'flights.parquet',
              'column_list': ['carrier', 'origin', 'destination', 'flight_num', 'flight_time', 'tail_num', 'dep_time', 'arr_time', 'dep_delay', 'arr_delay', 'taxi_out', 'taxi_in', 'distance', 'cancelled', 'diverted', 'id2'],
              'total_number_of_rows': 344827}}
    aircraft_models = requests.get('http://127.0.0.1:5000/aircrafts/')
    assert aircraft_models.status_code == 200
    assert aircraft_models.json()['0'] == {'manufacturer': 'WILCOX H L/WILCOX C N', 'model': 'CW', 'seats': 0}
    aircrafts = requests.get('http://127.0.0.1:5000/aircrafts/')
    assert aircrafts.status_code == 200
    aircraft_reports = requests.get('http://127.0.0.1:5000/aircrafts/')
    assert aircraft_reports.status_code == 200
    aircraft_reports_pivot = requests.get('http://127.0.0.1:5000/aircrafts/')
    assert aircraft_reports_pivot.status_code == 200


if __name__ == '__main__':
    api_test()
