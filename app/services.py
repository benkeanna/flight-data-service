import os

import pandas as pd
from pyarrow import parquet as pq

from extensions import cache


data_dir = os.path.join(os.path.dirname(__file__), 'data')
filenames = os.listdir(data_dir)


def get_parquet_data():
    pd_data = {}
    for filename in filenames:
        data = cache.get(filename)
        if not data:
            load()
            data = cache.get(filename)
        pd_data[filename] = data
    return pd_data


def load():
    for filename in filenames:
        if not cache.get(filename):
            filepath = os.path.join(data_dir, filename)
            data = pq.read_table(filepath)
            cache.set(filename, data, timeout=86400)


def get_info():
    pq_data = get_parquet_data()
    info_dict = {'data_file_name': [], 'column_list': [], 'total_number_of_rows': []}
    for filename, data in pq_data.items():
        df = data.to_pandas()
        info_dict['data_file_name'].append(filename)
        info_dict['column_list'].append(df.columns.values.tolist())
        info_dict['total_number_of_rows'].append(len(df.index))
    return pd.DataFrame.from_dict(info_dict)


def get_aircraft_models():
    pq_data = get_parquet_data()
    models_df = pq_data['aircraft_models.parquet'].to_pandas()
    return models_df.filter(['model', 'manufacturer', 'seats'], axis=1)


def get_aircrafts_filtered(manufacturer=None, model=None):
    df = _get_active_aircraft_with_models_joined()
    df = df[['model', 'manufacturer', 'seats', 'aircraft_serial', 'name', 'county']]
    if manufacturer:
        df = df.loc[df['manufacturer'] == manufacturer]
    if model:
        df = df.loc[df['model'] == model]
    df.reset_index(drop=True, inplace=True)
    return df


def get_report():
    report_table = _get_model_and_manufacturer_counts_by_county()
    return report_table


def get_report_pivot():
    pivot_table = _get_model_and_manufacturer_counts_by_county()
    pivot_table.reset_index(inplace=True)
    pivot_table.fillna('NULL', inplace=True)
    return pivot_table


def _get_active_aircraft_with_models_joined():
    pq_data = get_parquet_data()
    models_df = pq_data['aircraft_models.parquet'].to_pandas()
    aircraft_df = pq_data['aircraft.parquet'].to_pandas()
    active_aircraft_df = aircraft_df.loc[aircraft_df['status_code'] == 'A']
    return pd.concat([models_df, active_aircraft_df], axis=1, join="inner")


def _get_model_and_manufacturer_counts_by_county():
    df = _get_active_aircraft_with_models_joined()
    return df.pivot_table(index=['manufacturer', 'model'], columns='county', values='id', aggfunc='count')
