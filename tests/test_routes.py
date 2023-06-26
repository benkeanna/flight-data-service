import pytest

from app import app


@pytest.fixture
def client():
    app.testing = True
    with app.test_client() as client:
        yield client


def test_info(client):
    response = client.get('/info/')
    assert response.status_code == 200
    assert response.json == {
        '0': {'data_file_name': 'aircraft.parquet',
              'column_list': ['id', 'tail_num', 'aircraft_serial', 'aircraft_model_code', 'aircraft_engine_code',
                              'year_built', 'aircraft_type_id', 'aircraft_engine_type_id', 'registrant_type_id', 'name',
                              'address1', 'address2', 'city', 'state', 'zip', 'region', 'county', 'country',
                              'certification', 'status_code', 'mode_s_code', 'fract_owner', 'last_action_date',
                              'cert_issue_date', 'air_worth_date'],
              'total_number_of_rows': 3599},
        '1': {'data_file_name': 'aircraft_models.parquet',
              'column_list': ['aircraft_model_code', 'manufacturer', 'model', 'aircraft_type_id',
                              'aircraft_engine_type_id', 'aircraft_category_id', 'amateur', 'engines', 'seats',
                              'weight', 'speed'],
              'total_number_of_rows': 60461},
        '2': {'data_file_name': 'airports.parquet',
              'column_list': ['id', 'code', 'site_number', 'fac_type', 'fac_use', 'faa_region', 'faa_dist', 'city',
                              'county', 'state', 'full_name', 'own_type', 'longitude', 'latitude', 'elevation',
                              'aero_cht', 'cbd_dist', 'cbd_dir', 'act_date', 'cert', 'fed_agree', 'cust_intl',
                              'c_ldg_rts', 'joint_use', 'mil_rts', 'cntl_twr', 'major'],
              'total_number_of_rows': 19793},
        '3': {'data_file_name': 'carriers.parquet',
              'column_list': ['code', 'name', 'nickname'],
              'total_number_of_rows': 21},
        '4': {'data_file_name': 'flights.parquet',
              'column_list': ['carrier', 'origin', 'destination', 'flight_num', 'flight_time', 'tail_num', 'dep_time',
                              'arr_time', 'dep_delay', 'arr_delay', 'taxi_out', 'taxi_in', 'distance', 'cancelled',
                              'diverted', 'id2'],
              'total_number_of_rows': 344827}}


def test_aircraft_models(client):
    response = client.get('/aircrafts/models/')
    assert response.status_code == 200
    assert response.json['0'] == {'manufacturer': 'WILCOX H L/WILCOX C N', 'model': 'CW', 'seats': 0}


def test_aircrafts_filtered(client):
    response = client.get('/aircrafts/')
    assert response.status_code == 200
    assert len(response.json) == 1713
    assert response.json[0] == {'aircraft_serial': '11906', 'county': '013', 'manufacturer': 'WILCOX H L/WILCOX C N', 'model': 'CW', 'name': 'FORSBERG CHARLES P', 'seats': 0}


def test_aircrafts_filtered_by_manufacturer(client):
    response = client.get('/aircrafts/?manufacturer=STOKES')
    assert response.status_code == 200
    assert len(response.json) == 8
    assert response.json == {
        '0': {'model': 'JS56C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '15074910', 'name': 'STEPHANY PETER T', 'county': '011'},
        '1': {'model': 'JS56C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '58', 'name': 'FALCON AIR LINES INC', 'county': '095'},
        '2': {'model': 'JS77C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '051', 'name': 'VIALL WILLARD L', 'county': '021'},
        '3': {'model': 'JS77C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '61-0649', 'name': 'UNITED STATES AIR FORCE', 'county': '001'},
        '4': {'model': 'JS77C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '402C1020', 'name': 'HYANNIS AIR SERVICE INC', 'county': '001'},
        '5': {'model': 'JS77C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '32-704', 'name': 'MANCINI ANTONIO', 'county': '059'},
        '6': {'model': 'JS77C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '21059200', 'name': 'AITKEN GARY H', 'county': '059'},
        '7': {'model': 'JS1800H', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '76', 'name': 'WYBENGA JACK C', 'county': '085'}}


def test_aircrafts_filtered_by_model(client):
    response = client.get('/aircrafts/?model=JS56C')
    assert response.status_code == 200
    assert len(response.json) == 2
    assert response.json == {
        '0': {'model': 'JS56C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '15074910',
              'name': 'STEPHANY PETER T', 'county': '011'},
        '1': {'model': 'JS56C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '58',
              'name': 'FALCON AIR LINES INC', 'county': '095'}}


def test_aircrafts_filtered_by_model_and_manufacturer(client):
    response = client.get('/aircrafts/?manufacturer=STOKES&model=JS56C')
    assert response.status_code == 200
    assert len(response.json) == 2
    assert response.json == {
        '0': {'model': 'JS56C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '15074910', 'name': 'STEPHANY PETER T', 'county': '011'},
        '1': {'model': 'JS56C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '58', 'name': 'FALCON AIR LINES INC', 'county': '095'}}


def test_aircrafts_filtered_by_model_and_manufacturer_different(client):
    response = client.get('/aircrafts/?manufacturer=STOKES&model=002')
    assert response.status_code == 200
    assert len(response.json) == 0


def test_aircraft_reports(client):
    response = client.get('/aircrafts/reports/')
    assert response.status_code == 200


def test_aircraft_reports_pivot(client):
    response = client.get('/aircrafts/reports/pivot/')
    assert response.status_code == 200
