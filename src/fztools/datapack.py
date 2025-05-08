
from .validation import validate_data, show_example
from collections import UserDict
import pandas as pd
import geopandas as gpd
import warnings
from colorama import Fore, Style
from itertools import chain

class DataPack(UserDict):
    __slots__ = ['_vali_dict', '_vali_status']

    def __init__(self, data):
        super().__init__(data)
        self._vali_status = 0

    def __repr__(self):
        repr_str = ''
        for k,v in self.data.items():
            repr_str += f'{k}: {v.shape[0]}\n'
        return repr_str
    def _repr_dataframe_(self):
        '''a data frame representation of the data pack'''
        repr_row = []
        has_remove_ = False
        for k,v in self.data.items():
            has_remove_ = True if k == '_remove_' else has_remove_
            row_count = v.shape[0]
            is_geopandas = isinstance(v, gpd.GeoDataFrame)
            has_geometry_column = 'geometry' in v.columns
            geometry_is_geom = isinstance(v.geometry, gpd.GeoSeries) if has_geometry_column else False

            contain_geometry = is_geopandas and has_geometry_column and geometry_is_geom
            crs = v.geometry.crs if contain_geometry else 'None'
            repr_row += [(k, row_count, contain_geometry, crs)]

        # parse connected row
        df = pd.DataFrame(repr_row
                            , columns=['Data', 'Row Count', 'Geo', 'CRS'])#.set_index('Data')
        
        # add a indicator column to show rows that failed validation check
        if has_remove_ and (self._vali_status == 1):
            col_remove = self.data['_remove_'].groupby('filename').agg(
                { 'idx': lambda x: list(chain(*x)) }
            ).reset_index()
            df = df.merge(col_remove, left_on = 'Data', right_on = 'filename', how = 'left')
            # parse validation status
            df['Validation'] = df['idx']\
                .case_when([
                    (df['idx'].isna() & df['Data'].isin(self.vali_dict.keys()), 'Passed'),
                    (df['idx'].apply(lambda x: isinstance(x, list) and len(x) > 0), 'Failed'),
                    (df['idx'].isna(), '-')
                ])
            df['No. Failed'] = df['idx'].case_when([
                (df['idx'].isna(), 0),
                (df['idx'].notna(), df['idx'].apply(lambda x: len(x) if isinstance(x, list) else 0))
            ])
            df = df.drop(columns=['idx','filename'])
        else:
            df['Validation'] = '-'
        

        return df
    def _repr_html_(self):
        return self._repr_dataframe_().style.set_caption('A Directory of DataFrames').to_html()
    

    @property
    def vali_dict(self):
        return self._vali_dict
    @vali_dict.setter
    def vali_dict(self, dict):
        self.add_validation(dict)
    
    def add_validation(self, dict):
        self._vali_dict = {}
        for i in dict.keys():
            if i not in self.data.keys():
                warnings.warn(Fore.YELLOW + f'{i} not in data' + Style.RESET_ALL)
            self._vali_dict[i] = dict[i]
        return self
        
    def validate(self):
        new_instance = DataPack(validate_data(self.data, self.vali_dict))
        new_instance._vali_status = 1
        new_instance.add_validation(self.vali_dict)
        return new_instance
    
    def find_discrepancy(self, key):
        ect = show_example(self.data)[key]
        return ect