from src.fztools import StageManager
import igraph as ig
import pandas as pd
import uuid

def test_stagemanager():
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
    print(stage2.output)

    print(stage1.funcs)
    print(stage1.funcs_args)
    print(chain.output)
    print(chain)

    # try create a dependency graph;

    print(chain.igraph.vs[0])

    print(chain.to_mermaid_code())

    print(chain.node_edge[1])
    print(chain.edge_table)
# importing the multiprocessing module
import multiprocessing

def print_cube(num):
    """
    function to print cube of given num
    """
    print("Cube: {}".format(num * num * num))

def print_square(num):
    """
    function to print square of given num
    """
    print("Square: {}".format(num * num))

if __name__ == "__main__":
    import asyncio
    async def main():
        await asyncio.sleep(2)
        print('hello')

    asyncio.run(main())
    
    