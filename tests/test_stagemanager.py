
import pandas as pd
from fztools.stagemanager import StageManager

def test_reshaper():
    
    # initiate 
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