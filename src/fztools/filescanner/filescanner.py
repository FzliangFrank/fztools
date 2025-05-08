
import pandas as pd
import geopandas as gpd
from collections import UserDict
from functools import chain
from colorama import Fore, Style
from ..datapack import DataPack

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
        """
        download a given file from this frame
        """
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

