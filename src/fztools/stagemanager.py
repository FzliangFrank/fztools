
"""
Reuse dataframe Namespace with `StageManager`

## Reuse dataframe or anytable namespace to do data wrangling

Basic Use Case:
```
# register a function to this input
@stage_manager.register("Chamber")
def sum_chamber(df):
    # df is Chamber from input
    return df.groupby("town")[["qty"]].sum().reset_index()

# register this function to a new target output TownSum from input data Chamber Duct
@stage_manager.register("TownSum", ["Chamber","Duct"])
def sum_by_town(df1, df2):
    df = pd.merge(df1, df2, on=["town","id"], how="outer")
    return df.groupby("town")[["length", "qty"]].sum().reset_index()
```

## MPV

```py
# first initiate a stage manager 
stage_manager = StageManager(label="Sum by Cabinet")

@stage_manager.register("Chamber")
def sum_chamber(df):
    # df is Chamber from input
    return df.groupby("town")[["qty"]].sum().reset_index()

@stage_manager.register("Duct")
def sum_duct(df):
    # df is Duct from input
    return df.groupby("town")[["length"]].sum().reset_index()


@stage_manager.register("TownSum", ["Chamber","Duct"])
def sum_by_town(df1, df2):
    df = pd.merge(df1, df2, on=["town","id"], how="outer")
    return df.groupby("town")[["length", "qty"]].sum().reset_index()


# you have a directory of dataframe
input_dict = {
    "Chamber": pd.DataFrame({"id": range(1,5),"qty":[1,2,3,4],"town":["a", "a", "b", "b"]}),
    "Duct": pd.DataFrame({"id": range(1,5),"length": [0.75,0.5,0.5,0.25],"town":["a", "a", "b", "b"]})
}

# now register this directory 
stage_manager.input = input_dict

# now you can call this same function to get the value you want
stage_manager("Duct")
stage_manager("Chamber)
stage_manager("TownSum)
```
"""


from typing import Any, List, Dict, Callable, Optional, Union
from collections import defaultdict
import pandas as pd
import geopandas as gpd
from functools import wraps

class StageManager():
    '''
    method:
        register: register a function to a new target output; 
    properties:
        input: dict of variables
        output: dict of dataframe
        funcs: dict of function
        funcs_args: dict of list of string
        pass_input: bool
        name: string
        next: StageManager
        prev: StageManager
    
    details:
        You can chain stage managers with `>>` operator.
    '''
    __slots__ = [  "input"
                 , "output"
                 , "funcs"
                 , "funcs_args"
                 , "pass_input"
                 , "name"
                 , "next"
                 , "prev" ]
    def __init__(self, input:dict={}, name="", pass_input=True):
        self.input: Dict[str, List[Union[pd.DataFrame, gpd.GeoDataFrame]]] = input
        self.output: Dict[str, List[Union[pd.DataFrame, gpd.GeoDataFrame]]] = {}
        self.funcs: Dict[str,Callable] = {}
        self.funcs_args: Dict[str,List[str]] = {}
        self.pass_input:bool = pass_input
        self.name = name
    def register(self, OutputNs:str, InputNs:Optional[List[str]]=None):
        """
        register a function to new target output; better used as decorator. 
        When this funcion only have `OutputNs`, in the next stage the same namesapce will be 
        produced
        """
        if InputNs is None:
                InputNs = [OutputNs]
        def collect_func(func):
            self.funcs[OutputNs] = func
            self.funcs_args[OutputNs] = InputNs
            return func
        return collect_func
    def invoke_all(self):
        input_keys = list(self.input.keys())
        # invoke all the functions
        for ns in self.funcs.keys():
            self.invoke(ns)
            if ns in input_keys:
                input_keys.remove(ns)
        # add the rest of the input to output
        if self.pass_input:
            for ns in input_keys:
                self.output[ns] = self.input[ns]
        
        return self
    def invoke(self, OutputNs:str
                 , **kwds: Any) -> Any:
        """
        invoke the function registered with that `OutputNs`and return the output
        - `OutputNs`: string, the namespace of the output variable
        """
        if OutputNs not in self.funcs:
            output = self.input[OutputNs]
        else:
            func = self.funcs[OutputNs]
            func_args_ns = self.funcs_args[OutputNs]
            func_args = [self.input[ns] for ns in func_args_ns]
            output = func(*func_args, **kwds)
        self.output[OutputNs] = output
        return output
    def __call__(self, OutputNs:str
                 , **kwds: Any) -> Any:
        """
        invoke the function registered with that `OutputNs`and return the output
        - `OutputNs`: string, the namespace of the output variable
        """
        output = self.invoke(OutputNs, **kwds)
        return output
    def __repr__(self):
        return self.name
    def __rshift__(self, other):
        assert isinstance(other, self.__class__), "other must be a StageManager"
        self.next = other
        other.prev = self
        return other
    def invoke_forward(self):
        self.invoke_all()
        if hasattr(self, "next"):
            self.next.input = self.output.copy()
            return self.next.invoke_forward()
        else:
            return self
    def invoke_backward(self):
        if hasattr(self, "prev"):
            self.prev.invoke_backward()
        else:
            self.invoke_forward()
        return self
