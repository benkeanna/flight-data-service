import pandas as pd


def get_info(pq_data):
    info_dict = {'data_file_name': [], 'column_list': [], 'total_number_of_rows': []}
    for filename, data in pq_data.items():
        pandas_table = data.to_pandas()
        info_dict['data_file_name'].append(filename)
        info_dict['column_list'].append(pandas_table.columns.values.tolist())
        info_dict['total_number_of_rows'].append(len(pandas_table.index))
    info = pd.DataFrame.from_dict(info_dict)
    return info


def get_aircrafts_filtered(pq_data):
    models_table = pq_data['aircraft_models.parquet'].to_pandas()
    aircrafts_table = pq_data['aircraft.parquet'].to_pandas()
    info_dict = models_table.filter(['model', 'manufacturer', 'seats'], axis=1)
    return info_dict


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
