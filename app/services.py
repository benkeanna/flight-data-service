import pandas as pd


def get_info(pq_data):
    info_dict = {'data_file_name': [], 'column_list': [], 'total_number_of_rows': []}
    for filename, data in pq_data.items():
        df = data.to_pandas()
        info_dict['data_file_name'].append(filename)
        info_dict['column_list'].append(df.columns.values.tolist())
        info_dict['total_number_of_rows'].append(len(df.index))
    info = pd.DataFrame.from_dict(info_dict)
    return info


def get_aircraft_models(pq_data):
    models_df = pq_data['aircraft_models.parquet'].to_pandas()
    result_df = models_df.filter(['model', 'manufacturer', 'seats'], axis=1)
    return result_df


def get_aircrafts_filtered(pq_data, manufacturer=None, model=None):
    models_df = pq_data['aircraft_models.parquet'].to_pandas()
    aircraft_df = pq_data['aircraft.parquet'].to_pandas()
    active_aircraft_df = aircraft_df.loc[aircraft_df['status_code'] == 'A']
    result_df = pd.concat([models_df, active_aircraft_df], axis=1, join="inner")
    result_df = result_df[['model', 'manufacturer', 'seats', 'aircraft_serial', 'name', 'county']]
    if manufacturer:
        result_df = result_df.loc[result_df['manufacturer'] == manufacturer]
    if model:
        result_df = result_df.loc[result_df['model'] == model]
    result_df.reset_index(drop=True, inplace=True)
    return result_df


def get_report(pq_data):
    models_table = pq_data['aircraft_models.parquet'].to_pandas()
    aircrafts_table = pq_data['aircraft.parquet'].to_pandas()
    info_dict = models_table.filter(['model', 'manufacturer', 'seats'], axis=1)
    return info_dict


def get_report_pivot(pq_data):
    models_table = pq_data['aircraft_models.parquet'].to_pandas()
    aircrafts_table = pq_data['aircraft.parquet'].to_pandas()
    info_dict = models_table.filter(['model', 'manufacturer', 'seats'], axis=1)
    return info_dict
