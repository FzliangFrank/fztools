from src.fztools import StageManager

if __name__ == "__main__":
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
    stage1 >> stage2
    stage1.invoke_forward()
    print(stage2.output)

    print(stage1.funcs)
    print(stage1.funcs_args)

      
      