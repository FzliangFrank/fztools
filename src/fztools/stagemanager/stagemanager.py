
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


from .mx_igraph import iGraphMixin
from .mx_mermaid import MermaidMixin
from .mx_async import AsyncMixin
from ..datapack import DataPack

from typing import Any, List, Dict, Callable, Optional, Union, Iterator
from collections import defaultdict
import pandas as pd
import geopandas as gpd
from functools import wraps
import igraph as ig
import uuid
from IPython.display import HTML
from typing import List, Union
import pandas as pd
from functools import reduce

from typing import overload



class StageManager():
    '''
    method:
        - register: register a function to a new target output; 
        - invoke: invoke the function registered with that `OutputNs`and return the output
        - __call__: same as invoke
        - __rshift__: chain two stage managers together as in `stage_manager1 >> stage_manager2`
        - invoke_forward: invoke all the functions registered within the stage manager
        - invoke_backward: invoke all the functions registered within the stage manager in reverse order

    property:
        - input: dict of variables
        - output: dict of dataframe that is the result of the calculation the the functions being called;
        - funcs: dict of function
        - funcs_args: dict of list of string
        - pass_input: bool
        - name: string
        - next: StageManager
        - prev: StageManager
    
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
    def __init__(self, input:dict=defaultdict(lambda: None), name="stage", pass_input=True):
        self.input: Dict[str,Union[Any,DataPack]] = input
        self.output: Dict[str, Union[Any,DataPack]] = defaultdict(lambda: None)
        self.funcs: Dict[str,Callable] = defaultdict(lambda: None)
        self.funcs_args: Dict[str,List[str]] = defaultdict(list)
        self.pass_input:bool = pass_input # an option to allow pass input to the next stage;
        self.name = name

    def purne(self):
        self.input = defaultdict(dict)
        self.output = defaultdict(dict)
        return self
    def register(self, OutputNs:str, InputNs:Optional[List[str]]=None):
        """
        Register a function to new target output; better used as decorator. 
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
        """
        Execute all the functions registered within the stage manager
        """
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
        # if output namespace is not registered then look for input namespace;
        if OutputNs not in self.funcs:
            output = self.input[OutputNs]
        else:
            func = self.funcs[OutputNs]
            func_args_ns = self.funcs_args[OutputNs]
            func_args = [self.input[ns] for ns in func_args_ns]
            output = func(*func_args, **kwds)
        self.output[OutputNs] = output
        # pass this output to next stage
        if hasattr(self, "next"):
            self.next.input = self.output.copy()
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
    def __rshift__(self, other)->'StageChain':
        """
        Chain two stage managers together as in `stage_manager1 >> stage_manager2`
        """
        assert isinstance(other, self.__class__), "other must be a StageManager"
        return StageChain([self, other])
    def chain(self, other):
        """
        Chain two stage managers together
        """
        assert isinstance(other, self.__class__), "other must be a StageManager"
        self.next = other
        other.prev = self
    def invoke_forward(self):
        """
        Invoke all the functions registered within the stage manager,
        and invoke the next stage manager if it exists.
        """
        self.invoke_all()
        if hasattr(self, "next"):
            self.next.input = self.output.copy()
            return self.next.invoke_forward()
        else:
            return self
    def invoke_backward(self):
        """
        Simply find the first stage and then invoke forward
        """
        if hasattr(self, "prev"):
            self.prev.invoke_backward()
        else:
            self.invoke_forward()
        return self

class StageChain(iGraphMixin, MermaidMixin, AsyncMixin):
    """
    StageChain is a collection of stage managers that are chained together using `>>` operator.

    method:
        - __rshift__: extend the chain by adding more stage managers together as in `stage_manager1 >> stage_manager2`
        - invoke: invoke all the functions registered within the stage manager
    """
    _stages: List[StageManager] = []
    def __init__(self, stages:List[StageManager]):
        self.stages = stages
    def __repr__(self):
        chain_str = " >> ".join([stage.name + '(' + str(i+1) + ')' for i, stage in enumerate(self._stages)])
        return f"StageChain({chain_str})"
    
    def __getitem__(self, key:int)->StageManager:
        return self._stages[key]
    
    def __len__(self)->int:
        return len(self._stages)
    
    def __iter__(self)->Iterator[StageManager]:
        return iter(self._stages)

    @overload
    def __rshift__(self, other:StageManager)->'StageChain':...
    
    @overload
    def __rshift__(self, other:'StageChain')->'StageChain':...

    def __rshift__(self, other:Union[StageManager, 'StageChain'])->'StageChain':
        if isinstance(other, StageManager):
            self._stages[-1] >> other
            self._stages.append(other)
        elif isinstance(other, StageChain):
            self._stages[-1] >> other._stages[0]
            self._stages.extend(other._stages)
        else:
            raise TypeError(f"other must be a StageManager or StageChain, not {type(other)}")
        return self
    

    @property
    def stages(self):
        return self._stages
    
    @stages.setter
    def stages(self, stages:List[StageManager]):
        reduce(lambda x, y: x.chain(y), stages)
        self._stages = stages
    
    @property
    def input(self):
        """
        Return the input of the first stage manager;
        """
        return self._stages[0].input
    
    @input.setter
    def input(self, input:dict):
        """
        Set the input of the first stage manager;
        """
        self._stages[0].input = input
    
    @property
    def output(self):
        """
        Return the output of calculation result;
        """
        return self._stages[-1].output
    
    def invoke(self):
        self._stages[0].invoke_forward()
        return self
    
    def purne(self):
        for sg in self._stages:
            sg.purne()
        return self