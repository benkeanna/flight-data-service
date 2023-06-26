import os
import pandas as pd
from pyarrow import parquet as pq


class DataRepository:
    def __init__(self, cache):
        self.cache = cache
        self.data_dir = os.path.join(os.path.dirname(__file__), 'data')
        self.filenames = os.listdir(self.data_dir)

    def _get_parquet_data(self):
        pd_data = {}
        for filename in self.filenames:
            data = self.cache.get(filename)
            if not data:
                self._load()
                data = self.cache.get(filename)
            pd_data[filename] = data
        return pd_data

    def _load(self):
        for filename in self.filenames:
            if not self.cache.get(filename):
                filepath = os.path.join(self.data_dir, filename)
                data = pq.read_table(filepath)
                self.cache.set(filename, data, timeout=86400)

    def get_info(self):
        pq_data = self._get_parquet_data()
        info_dict = {'data_file_name': [], 'column_list': [], 'total_number_of_rows': []}
        for filename, data in pq_data.items():
            df = data.to_pandas()
            info_dict['data_file_name'].append(filename)
            info_dict['column_list'].append(df.columns.values.tolist())
            info_dict['total_number_of_rows'].append(len(df.index))
        return pd.DataFrame.from_dict(info_dict)

    def get_aircraft_models(self):
        pq_data = self._get_parquet_data()
        models_df = pq_data['aircraft_models.parquet'].to_pandas()
        return models_df.filter(['model', 'manufacturer', 'seats'], axis=1)

    def get_aircrafts_filtered(self, manufacturer=None, model=None):
        df = self._get_active_aircraft_with_models_joined()
        df = df[['model', 'manufacturer', 'seats', 'aircraft_serial', 'name', 'state']]
        if manufacturer:
            df = df.loc[df['manufacturer'] == manufacturer]
        if model:
            df = df.loc[df['model'] == model]
        df.reset_index(drop=True, inplace=True)
        return df

    def get_report(self):
        df = self._get_active_aircraft_with_models_joined()
        return df.groupby(['manufacturer', 'model', 'state']).size().reset_index(name='count')

    def get_report_pivot(self):
        df = self._get_active_aircraft_with_models_joined()
        summary_counts = df.groupby(['manufacturer', 'model', 'state']).size().reset_index(name='count')
        pivot_df = summary_counts.pivot(index=['manufacturer', 'model'], columns='state', values='count')
        pivot_df.fillna('NULL', inplace=True)
        pivot_df.reset_index(inplace=True)
        return pivot_df

    def _get_active_aircraft_with_models_joined(self):
        pq_data = self._get_parquet_data()
        models_df = pq_data['aircraft_models.parquet'].to_pandas()
        aircraft_df = pq_data['aircraft.parquet'].to_pandas()
        active_aircraft_df = aircraft_df.loc[aircraft_df['status_code'] == 'A']
        return pd.concat([models_df, active_aircraft_df], axis=1, join="inner")
