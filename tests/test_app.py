import json

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
    assert response.json == [
        {'data_file_name': 'aircraft.parquet',
         'column_list': ['id', 'tail_num', 'aircraft_serial', 'aircraft_model_code', 'aircraft_engine_code',
                         'year_built', 'aircraft_type_id', 'aircraft_engine_type_id', 'registrant_type_id', 'name',
                         'address1', 'address2', 'city', 'state', 'zip', 'region', 'county', 'country',
                         'certification', 'status_code', 'mode_s_code', 'fract_owner', 'last_action_date',
                         'cert_issue_date', 'air_worth_date'],
         'total_number_of_rows': 3599},
        {'data_file_name': 'aircraft_models.parquet',
         'column_list': ['aircraft_model_code', 'manufacturer', 'model', 'aircraft_type_id',
                         'aircraft_engine_type_id', 'aircraft_category_id', 'amateur', 'engines', 'seats',
                         'weight', 'speed'],
         'total_number_of_rows': 60461},
        {'data_file_name': 'airports.parquet',
         'column_list': ['id', 'code', 'site_number', 'fac_type', 'fac_use', 'faa_region', 'faa_dist', 'city',
                         'county', 'state', 'full_name', 'own_type', 'longitude', 'latitude', 'elevation',
                         'aero_cht', 'cbd_dist', 'cbd_dir', 'act_date', 'cert', 'fed_agree', 'cust_intl',
                         'c_ldg_rts', 'joint_use', 'mil_rts', 'cntl_twr', 'major'],
         'total_number_of_rows': 19793},
        {'data_file_name': 'carriers.parquet',
         'column_list': ['code', 'name', 'nickname'],
         'total_number_of_rows': 21},
        {'data_file_name': 'flights.parquet',
         'column_list': ['carrier', 'origin', 'destination', 'flight_num', 'flight_time', 'tail_num', 'dep_time',
                         'arr_time', 'dep_delay', 'arr_delay', 'taxi_out', 'taxi_in', 'distance', 'cancelled',
                         'diverted', 'id2'],
         'total_number_of_rows': 344827}]


def test_aircraft_models(client):
    response = client.get('/aircrafts/models/')
    assert response.status_code == 200
    assert response.json[0] == {'manufacturer': 'WILCOX H L/WILCOX C N', 'model': 'CW', 'seats': 0}


def test_aircrafts_filtered(client):
    response = client.get('/aircrafts/')
    assert response.status_code == 200
    assert len(response.json) == 1713
    assert response.json[0] == {'aircraft_serial': '11906', 'state': 'NH', 'manufacturer': 'WILCOX H L/WILCOX C N',
                                  'model': 'CW', 'name': 'FORSBERG CHARLES P', 'seats': 0}


def test_aircrafts_filtered_by_manufacturer(client):
    response = client.get('/aircrafts/?manufacturer=STOKES')
    assert response.status_code == 200
    assert len(response.json) == 8
    assert response.json == [
        {'model': 'JS56C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '15074910', 'name': 'STEPHANY PETER T', 'state': 'FL'},
        {'model': 'JS56C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '58', 'name': 'FALCON AIR LINES INC', 'state': 'MO'},
        {'model': 'JS77C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '051', 'name': 'VIALL WILLARD L', 'state': 'IL'},
        {'model': 'JS77C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '61-0649', 'name': 'UNITED STATES AIR FORCE', 'state': 'DC'},
        {'model': 'JS77C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '402C1020', 'name': 'HYANNIS AIR SERVICE INC', 'state': 'MA'},
        {'model': 'JS77C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '32-704', 'name': 'MANCINI ANTONIO', 'state': 'CA'},
        {'model': 'JS77C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '21059200', 'name': 'AITKEN GARY H', 'state': 'IN'},
        {'model': 'JS1800H', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '76', 'name': 'WYBENGA JACK C', 'state': 'TX'}
    ]


def test_aircrafts_filtered_by_model(client):
    response = client.get('/aircrafts/?model=JS56C')
    assert response.status_code == 200
    assert len(response.json) == 2
    assert response.json == [{'model': 'JS56C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '15074910', 'name': 'STEPHANY PETER T', 'state': 'FL'},
                             {'model': 'JS56C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '58', 'name': 'FALCON AIR LINES INC', 'state': 'MO'}]


def test_aircrafts_filtered_by_model_and_manufacturer(client):
    response = client.get('/aircrafts/?manufacturer=STOKES&model=JS56C')
    assert response.status_code == 200
    assert len(response.json) == 2
    assert response.json == [{'model': 'JS56C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '15074910', 'name': 'STEPHANY PETER T', 'state': 'FL'},
                             {'model': 'JS56C', 'manufacturer': 'STOKES', 'seats': 0, 'aircraft_serial': '58', 'name': 'FALCON AIR LINES INC', 'state': 'MO'}]


def test_aircrafts_filtered_by_model_and_manufacturer_different(client):
    response = client.get('/aircrafts/?manufacturer=STOKES&model=002')
    assert response.status_code == 200
    assert len(response.json) == 0


def test_aircraft_reports(client):
    response = client.get('/aircrafts/reports/')
    assert response.status_code == 200
    assert len(response.json) == 1700
    assert response.json[0] == {'count': 1, 'manufacturer': 'ABRON-HUBERT W JR', 'model': 'KR-1', 'state': 'TX'}


def test_aircraft_reports_pivot(client):
    response = client.get('/aircrafts/reports/pivot/')
    assert response.status_code == 200
    assert len(response.json) == 1624
    assert response.json[0] == {'manufacturer': 'ABRON-HUBERT W JR', 'model': 'KR-1', 'AK': 'NULL', 'AL': 'NULL',
                                'AP': 'NULL', 'AR': 'NULL', 'AZ': 'NULL', 'CA': 'NULL', 'CO': 'NULL', 'CT': 'NULL',
                                'DC': 'NULL', 'DE': 'NULL', 'FL': 'NULL', 'GA': 'NULL', 'GU': 'NULL', 'HI': 'NULL',
                                'IA': 'NULL', 'ID': 'NULL', 'IL': 'NULL', 'IN': 'NULL', 'KS': 'NULL', 'KY': 'NULL',
                                'LA': 'NULL', 'MA': 'NULL', 'MD': 'NULL', 'ME': 'NULL', 'MI': 'NULL', 'MN': 'NULL',
                                'MO': 'NULL', 'MS': 'NULL', 'MT': 'NULL', 'NC': 'NULL', 'ND': 'NULL', 'NE': 'NULL',
                                'NH': 'NULL', 'NJ': 'NULL', 'NM': 'NULL', 'NV': 'NULL', 'NY': 'NULL', 'OH': 'NULL',
                                'OK': 'NULL', 'OR': 'NULL', 'PA': 'NULL', 'PR': 'NULL', 'RI': 'NULL', 'SC': 'NULL',
                                'SD': 'NULL', 'TN': 'NULL', 'TX': 1.0, 'UT': 'NULL', 'VA': 'NULL', 'VI': 'NULL',
                                'VT': 'NULL', 'WA': 'NULL', 'WI': 'NULL', 'WV': 'NULL', 'WY': 'NULL'}


def test_data_by_sql_string(client):
    response = client.post('/sql/', data=json.dumps({"sql": "SELECT * FROM aircraft;"}), headers={"Content-Type": "application/json"})
    assert response.status_code == 200
    assert response.json[0] == {'id': 100, 'tail_num': 'N10036', 'aircraft_serial': '11906',
                                'aircraft_model_code': '7100510', 'aircraft_engine_code': '17003', 'year_built': 1944,
                                'aircraft_type_id': 4, 'aircraft_engine_type_id': 1, 'registrant_type_id': 1,
                                'name': 'FORSBERG CHARLES P', 'address1': 'PO BOX 1', 'address2': None,
                                'city': 'NORTH SUTTON', 'state': 'NH', 'zip': '03260-0001', 'region': 'E',
                                'county': '013', 'country': 'US', 'certification': '1N', 'status_code': 'A',
                                'mode_s_code': '50003624', 'fract_owner': None, 'last_action_date': '2006-01-17',
                                'cert_issue_date': '1982-04-27', 'air_worth_date': '1972-09-11'}
