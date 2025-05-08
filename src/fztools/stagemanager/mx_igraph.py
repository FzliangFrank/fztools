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
        node['label'] = node['element'].map(str)
        node['type'] = node['element'].map(intrap_type)
        return node, edge
    
    @property
    def edge_table(self):
        stages=self._stages

        output_input = [s.funcs_args for s in stages]
        # print(output_input)

        rows = []
        for i, stg in enumerate(output_input):
            j = i - 1
            for output, inputs in stg.items():
                func = stages[i].funcs[output]
                stage_name = stages[i].name
                
                row = (i, stage_name, output, j, inputs, func)
                rows.append(row)
        df = pd.DataFrame(rows, columns = ["stage_id", "stage_name", "output", "prev_stage_id", "inputs", "func"])
        # print(df)
        

        # collapsing the datafrmae
        cols = ['source_id', 'source_ele', 'target_id', 'target_ele']
        func2output = df[['stage_id','func','stage_id', 'output']].copy()
        func2output.columns = cols
        input2func = df[[ 'prev_stage_id', 'inputs', 'stage_id','func']].explode('inputs').copy()
        input2func.columns = cols

        edge_list = pd.concat([func2output, input2func])
        return edge_list
