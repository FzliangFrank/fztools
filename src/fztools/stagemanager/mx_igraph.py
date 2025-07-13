import igraph as ig
import pandas as pd
from typing import Callable

class iGraphMixin():
    @property
    def igraph(self):
        edge, node=self.edge_node
        g = ig.Graph.DataFrame(edge, directed=True, vertices=node, use_vids=True)
        return g
    
    @property
    def edge_node(self):
        node, edge=self.node_edge
        return edge, node
    
    @property
    def node_edge(self):

        cols = ['stage_id', 'element']
        edge_list = self.edge_table


        # form a node table;
        srcnode = edge_list[["source_id", "source_ele"]].copy()
        srcnode.columns = cols
        tgtnode = edge_list[["target_id", "target_ele"]].copy()
        tgtnode.columns = cols

        node_list = pd.concat([srcnode, tgtnode])
        node = node_list.drop_duplicates().reset_index(drop=True).reset_index(names='id')
        
        # make edges
        edge = edge_list.merge(
            node.rename(columns = {"id": "src", "stage_id":"source_id", "element": "source_ele"}), how = "left", on = ["source_id", "source_ele"]
        ).merge(
            node.rename(columns = {"id": "tgt", "stage_id":"target_id", "element": "target_ele"}), how = "left", on = ["target_id", "target_ele"]
        )[["src", "tgt"]]

        # additional column related with node
        def intrap_type(x):
            if isinstance(x, str):
                return "object"
            elif isinstance(x, Callable):
                return "function"
            else:
                return "Unknown"
        def make_label(x):
            if isinstance(x, str):
                return x
            elif isinstance(x, Callable):
                return x.__name__
            else:
                return str(x)
        node['label'] = node['element'].map(make_label)
        node['type'] = node['element'].map(intrap_type)
        return node, edge
    
    def as_table(self):
        """
        Show a table of input and output;
        """
        stages=self._stages

        output_input = [s.funcs_args for s in stages]
        # print(output_input)

        rows = []
        for i, stg in enumerate(output_input):

            # previous stage is just j - 1 this could be problematic
            # j = i - 1
            for output, inputs in stg.items():
                func = stages[i].funcs[output]
                stage_name = stages[i].name
                
                row = (i, stage_name, output,  inputs, func)
                rows.append(row)
        df = pd.DataFrame(rows, columns = ["stage_id", "stage_name", "output", "inputs", "func"])
        
        # left table for all the inputs;
        dfl = df.explode("inputs")

        # right table for all the outputs and its function;
        dfr = df[["output", "stage_id"]].rename(columns={"output": "inputs", "stage_id": "prev_stage_id"})

        # create the edge table by join back from the input;
        df = (dfl.merge(dfr, on=["inputs"], how="left") # input is from previous stage output
            .assign(prev_stage_id = lambda x: x["prev_stage_id"].fillna(-1).astype(int)) # if missing possible that come from input
            .query("prev_stage_id < stage_id or stage_id == 0") # only keep the ones that are connected
            .assign(prev_stage_id = lambda x: x["prev_stage_id"].mask(x["stage_id"] == 0, -1)) # stage id is -1 when 
            .sort_values("prev_stage_id", ascending=True)

            .groupby(["stage_id", "stage_name", "output", "inputs"])
            .agg("last") # access the last one in case of multiple output
            .reset_index()
            .assign(ns = lambda x: x["output"])
        )
        return df
    @property
    def edge_table(self):
        df = self.as_table()
        # print(df)
        

        # collapsing the datafrmae
        # function that produce output distinct on output and function
        cols = ['source_id', 'source_ele', 'target_id', 'target_ele']
        
        func2output = df[['stage_id','func','stage_id', 'output']].copy()
        func2output = func2output.drop_duplicates()
        func2output.columns = cols
        # input into function
        input2func = df[['prev_stage_id', 'inputs', 'stage_id','func']].explode('inputs')

        input2func.columns = cols

        edge_list = pd.concat([func2output, input2func])
        return edge_list
