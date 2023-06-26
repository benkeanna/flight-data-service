from __future__ import annotations

import os
import pandas as pd
from pandasql import sqldf
from pyarrow import parquet as pq


def faa_data_repository_factory(data_dir_path: str) -> FAADataRepository:
    filenames = os.listdir(data_dir_path)
    pd_data = {}
    for filename in filenames:
        filepath = os.path.join(data_dir_path, filename)
        data = pq.read_table(filepath)
        pd_data[filename] = data
    return FAADataRepository(pd_data)


class FAADataRepository:
    def __init__(self, data):
        self.pq_data = data

    def get_info(self) -> pd.DataFrame:
        info_dict = {'data_file_name': [], 'column_list': [], 'total_number_of_rows': []}
        for filename, data in self.pq_data.items():
            df = data.to_pandas()
            info_dict['data_file_name'].append(filename)
            info_dict['column_list'].append(df.columns.values.tolist())
            info_dict['total_number_of_rows'].append(len(df.index))
        return pd.DataFrame.from_dict(info_dict)

    def get_aircraft_models(self) -> pd.DataFrame:
        models_df = self.pq_data['aircraft_models.parquet'].to_pandas()
        return models_df.filter(['model', 'manufacturer', 'seats'], axis=1)

    def get_aircrafts_filtered(self, manufacturer: str = None, model: str = None) -> pd.DataFrame:
        df = self._get_active_aircraft_with_models_joined()
        df = df[['model', 'manufacturer', 'seats', 'aircraft_serial', 'name', 'state']]
        if manufacturer:
            df = df.loc[df['manufacturer'] == manufacturer]
        if model:
            df = df.loc[df['model'] == model]
        df.reset_index(drop=True, inplace=True)
        return df

    def get_report(self) -> pd.DataFrame:
        df = self._get_active_aircraft_with_models_joined()
        return df.groupby(['manufacturer', 'model', 'state']).size().reset_index(name='count')

    def get_report_pivot(self) -> pd.DataFrame:
        df = self._get_active_aircraft_with_models_joined()
        summary_counts = df.groupby(['manufacturer', 'model', 'state']).size().reset_index(name='count')
        pivot_df = summary_counts.pivot(index=['manufacturer', 'model'], columns='state', values='count')
        pivot_df.fillna('NULL', inplace=True)
        pivot_df.reset_index(inplace=True)
        return pivot_df

    def get_data_by_sql_string(self, sql_string: str) -> pd.DataFrame:
        dataframes = {}
        for name, data in self.pq_data.items():
            name, _ = name.split('.')
            dataframes[name] = data.to_pandas()
        return sqldf(sql_string, dataframes)

    def _get_active_aircraft_with_models_joined(self) -> pd.DataFrame:
        models_df = self.pq_data['aircraft_models.parquet'].to_pandas()
        aircraft_df = self.pq_data['aircraft.parquet'].to_pandas()
        active_aircraft_df = aircraft_df.loc[aircraft_df['status_code'] == 'A']
        return pd.concat([models_df, active_aircraft_df], axis=1, join="inner")
