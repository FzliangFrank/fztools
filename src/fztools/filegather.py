
import pandas as pd
import geopandas as gpd
from collections import UserDict
from functools import chain
from colorama import Fore, Style


class NestedFileFrame(pd.DataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __repr__(self):
        return '<NestedFileFrame>'

class FileFrame(pd.DataFrame):
    '''
    a special dataframe for capture files found on your disk;
    '''
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    @property
    def stats(self):
        return {
            'ac': self[['region', 'ac']].drop_duplicates().shape[0],
            'total_files': self.shape[0],
            'total_size': self['file_size'].sum() / 1024 / 1024,
        }
    @property
    def stats_str(self):
        return f'{self.stats["ac"]} ACs, {self.stats["total_files"]} files, {self.stats["total_size"]:.2f} MB'
    
    
    def query(self, expr: str,**kwargs):
        return FileFrame(super().query(expr,**kwargs))
    
    def _repr_html_(self):
        return self.head(5).style.set_caption(self.stats_str).to_html()

    def nest(cls):
        # file_df = self.query('~archive & is_phoenix ')
        file_df = cls
        gid = [ 'region', 'ac','version', 'run', 'project']
        return NestedFileFrame(file_df
                .groupby(gid)
                .agg({  'file_name': 'count'
                      , 'file_parent_id': 'nunique'
                      , 'file_modified': 'max'
                      , 'file_path': lambda x: x.to_list()
                      , 'file_size': 'sum'
                      })
                .sort_index()
                .eval('file_size = file_size / 1024 ')
                .rename(columns={'file_name': 'file_count'
                                 , 'file_parent_id': 'folder_count'
                                 , 'file_modified': 'last_update'
                                 , 'file_path': 'file_pathes'
                                 , 'file_size': 'file_size_kb'}))
    def download(self):
        for index, row in self.iterrows():
            file_path = row['file_path']
            if '.shp' in file_path.suffixes:
                data = gpd.read_file(file_path, engine='pyogrio', use_arrow=True)
            elif '.csv' in file_path.suffixes:
                data = pd.read_csv(file_path)
            else:
                raise ValueError(f'Unsupported file type: {file_path}')
            yield index, data
    def download_to_dict(self, key_from:str = 'file_name'):
        data_dict = {}
        if key_from not in self.columns:
            raise ValueError(f'{key_from} not in columns')
        if self[key_from].duplicated().any():
            raise ValueError(f'{key_from} has duplicated values')
        for index, data in self.download():
            key = self.loc[index, key_from]
            data_dict[key] = data
        return DataPack(data_dict)
    def __repr__(self):
        return '<FileFrame>'




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
        # print(ect[['_check', '_column']].drop_duplicates())
        return ect