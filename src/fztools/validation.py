from colorama import Fore, Style
from typing import List
import pandas as pd
import warnings

def contain_list_fullstop( tocheck:List
                         , required:List
                         , msg_prefix:str)->None:
    '''
    '''
    missing_items = [item for item in required if item not in tocheck]
    missing_items_str = ', '.join('"' + item + '"' for item in missing_items)
    if len(missing_items) != 0:
        raise ValueError(f"{msg_prefix}: {missing_items_str}")

def validate_data(  datadict:dict
                  , validation_manifasto:dict
                  , msg = 'data')->dict:
    '''Return original file directory and throw hard or soft error;
    - Hard error full stop process from executing all together
    - Soft error add a `_removed_` to the directory 

    **parameters**
    - `datadict`: 
    - `validation_manifasto`: a dictionary that share the same key as datadict, with its value being 
     key valud pair that are defined as:
        - 'unique-cols': list with each check corresponding column(s) are unique (if pair nest list within list) 
        - 'not-null': 
    - `msg`: pass to pretty print. 
    '''
    # check all the file exists - HARD
    specified_tables = validation_manifasto.keys()
    tables = datadict.keys()
    
    try:
        contain_list_fullstop(tables,specified_tables, Fore.RED + "\nMissing following tables")
    except:
        validation_manifasto = rename_key(validation_manifasto,"Net_Pathways_Distribution.shp","Net_Pathways.shp",)
        
        specified_tables = validation_manifasto.keys()

        contain_list_fullstop(tables,specified_tables, Fore.RED + "\nMissing following tables")
    
    # check all the required column exists - HARD
    for fileName, commands in validation_manifasto.items():
        req_cols = []
        for command, cols in commands.items():
            if isinstance(cols, List):
                single_cols = [col for col in cols if isinstance(cols, str)]
                paired_cols = [list(col) for col in cols if isinstance(cols, tuple)]
                req_cols += (single_cols + paired_cols)
        data = datadict.get(fileName)
        data_cols = data.columns
        contain_list_fullstop(data_cols, req_cols, Fore.RED + f"\nfile {fileName} don't contain following column")
    
    # internal check 
    _remove_ = []
    _reference_ = []
    for fileName, commands in validation_manifasto.items():
        data = datadict.get(fileName)
        for command, content in commands.items():
            if command == 'unique-cols':
                # look for duplicated column in each that are listed
                for col in content:
                    col = list(col) if isinstance(col, tuple) else col
                    idx = data[col][data[col].duplicated()].index
                    # if duplicated add to remove
                    if len(idx) != 0:
                        _remove_ += [(fileName, command, col, idx)]
                        msg = Fore.YELLOW + f"'{fileName}' contain {len(idx)} duplicated {col}!"
                        warnings.warn(UserWarning(msg))
                        
            elif command == 'not-null':
                for col in content:
                    col = list(col) if isinstance(col, tuple) else col
                    idx = data[col][data[col].isnull()].index
                    if len(idx) != 0:
                        _remove_ += [(fileName, command, col, idx)]
                        msg = Fore.YELLOW + f"'{fileName}' contain {len(idx)} missing {col}!"
                        warnings.warn(UserWarning(msg))
            elif command == 'check':
                for name, func in content.items():
                    try:
                        idx_ = func(data).index
                        idx = data.index[~data.index.isin(idx_)] # not in the current index
                    except Exception as e:
                        print(Fore.RED + "Validation Function Error for %s, %s, %s:"%(fileName, command, name))
                        raise e
                    if len(idx) != 0:
                        _remove_ += [(fileName, command, name, idx)]
                        msg = Fore.YELLOW + f"'{fileName}' contain {len(idx)} not fit custom requirement '{name}'!"
                        warnings.warn(UserWarning(msg))
            elif command == 'check-against':
                for name, func in content.items():
                    try:

                        idx = func(data).index
                    except Exception as e:
                        print(Fore.RED + "Validation Function Error for %s, %s, %s:"%(fileName, command, name))
                        raise e
                    if len(idx) != 0:
                        _remove_ += [(fileName, command, name, idx)]
                        msg = Fore.YELLOW + f"'{fileName}' contain {len(idx)} not fit custom requirement '{name}'!"
                        warnings.warn(UserWarning(msg))
            elif command == 'reference':
                for col, tblCol in content.items():
                    tbl_col = [(otherTbl, otherCol) for otherTbl, otherCol in tblCol.items()]
                    _reference_ += [(fileName, col, tbl_col)]
                    
            else:
                warnings.warn(Fore.YELLOW + f"Command '{command}' is not valid, do you have a typo?"\
                              + " hint: ('unique-cols', 'not-null', 'check','reference' )")

    # check for references after checking for required column and show
    _no_reference_ = []
    for row in _reference_:
        mainTbl, mainFK, references = row
        if isinstance(mainFK, tuple):
            mainFK = list(mainFK)
        # print(references)
        dim_vec = pd.concat([datadict[dimTbl][dimPK] for dimTbl, dimPK in references])
        fk_col = datadict[mainTbl][mainFK]

        no_reference_idx = fk_col[~fk_col.isin(dim_vec)].index
        if no_reference_idx.shape[0] != 0:
            _no_reference_ += [(mainTbl, 'reference', mainFK, no_reference_idx)]

    _remove_ = pd.DataFrame(_remove_ + _no_reference_, columns = ["filename","check","column", "idx"])

    datadict["_remove_"] = _remove_

    pretty_print__remove_(_remove_, msg=msg)

    # return the sanitized data dictionary;
    return datadict

def fix_file(datadict:dict)->dict:
    if "_remove_" in datadict.keys():
        # those file require removal
        _remove_=datadict["_remove_"]
        # removing rows;
        _remove_.apply(lambda x: datadict[x.filename].drop(x.idx, axis=0, inplace=True), axis=1)
        
    return datadict

def pretty_print__remove_(_remove_, msg = 'data'):
    if _remove_.shape[0] != 0:
        print(
            Fore.RED,
            "Following rows removed from corresponding table(s):\n",
            _remove_.assign(
                n_removed = lambda x: x.idx.apply(lambda x: x.shape[0]),
                remved_idx = lambda x: x.idx\
                    .apply(lambda e: ','.join( [str(j) for i,j in enumerate(e.tolist()) if i < 3 ]  ) ) # keep only two if higher than three     
                )
            .assign(remved_idx = lambda x: x.remved_idx\
                    .mask(x.n_removed >= 3, x.remved_idx + '...'))
            .assign(remved_idx = lambda x: x.remved_idx + ' (' + x.n_removed.astype('str') + ')')
            .drop(columns=["idx",'n_removed']),
            Style.RESET_ALL
        )
    else:
        print(Fore.GREEN, f"Validating {msg} passed" + Fore.RESET)


def show_example(data:dict)->dict:
    '''
    '''
    if '_remove_' in data.keys():
        _remove_ = data['_remove_']
        
        shows = {}
        for index, row in _remove_.iterrows():
            idx = row['idx']
            data_name = row['filename']
            shows[data_name] = data[data_name].loc[idx,:]
        return shows
    else:
        return None
