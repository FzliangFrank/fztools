
import pytest
import pandas as pd
from fztools.stagemanager.stagemanager import StageManager, StageChain



def test_chaining_types():
    stage1 = StageManager()
    stage2 = StageManager()
    stage3 = StageManager()

    class A():
        pass
    a = A()

    chain = stage1 >> stage2
    assert isinstance(chain, StageChain), f"chain should be a StageChain, not {type(chain)}"
    
    new_chain = chain >> stage3
    assert isinstance(new_chain, StageChain), f"new_chain should be a StageChain, not {type(new_chain)}"

    with pytest.raises(TypeError):
        chain >> a


def test_evaulation():
    
    # initiate 
    stage_manager = StageManager(name="Sum by Cabinet")

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


    # this is how any input has worked
    input_dict = {
        "Chamber": pd.DataFrame({"id": range(1,5),"qty":[1,2,3,4],"town":["a", "a", "b", "b"]}),
        "Duct": pd.DataFrame({"id": range(1,5),"length": [0.75,0.5,0.5,0.25],"town":["a", "a", "b", "b"]}),
        "_Ids": pd.DataFrame({"id": range(1,5)})
    }

    stage_manager.input = input_dict
    # test if the function is working
    assert stage_manager("Duct").equals(pd.DataFrame({
        "town": ["a", "b"],
        "length": [1.25, 0.75]
    }))
    assert stage_manager("Chamber").equals(pd.DataFrame({
        "town": ["a", "b"],
        "qty": [3,7]
    }))
    assert stage_manager("TownSum").equals(pd.DataFrame({
        "town": ["a", "b"],
        "length": [1.25, 0.75],
        "qty": [3,7]
    }))
    assert stage_manager("_Ids").equals(pd.DataFrame({"id": range(1,5)})), "When no function is registered, it should return the input"

def test_manager_chaining():
    stage1 = StageManager()
    stage2 = StageManager()

    input_dict = {
        "AplusOne": 1,
        "BtoPowerTwo": 2,
    }
    
    @stage1.register("AplusOne")
    def plus_one(a):
        return a + 1
    
    @stage1.register("BtoPowerTwo")
    def power_two(b):
        return b ** 2
    
    @stage2.register("CSumAB", ["AplusOne", "BtoPowerTwo"])
    def sum_all(a, b):
        return a + b
    
    stage1.input = input_dict
    chain = stage1 >> stage2
    chain.invoke()
    assert chain.output == {'CSumAB': 6, 'AplusOne': 2, 'BtoPowerTwo': 4}

    