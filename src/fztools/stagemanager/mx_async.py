from collections import defaultdict

from functools import reduce
import asyncio
from colorama import Fore


class AsyncMixin:
    """
    Mixin for asynchronous invocation of stage managers;
    
    method:
        - invoke_async: invoke all the functions registered within the stage manager asynchronously

    property:
        - dependency_dict: a dictionary of dependencies for each stage
        - deps: shortcut for dependency_dict


    example:
        ```python
        chain = StageChain(stages)
        await chain.invoke_async()
        ```

    You can set the `await_interval` and `await_maxtime` to control the interval and maximum time to wait for dependencies.
    You can set the `verbose` to control the verbosity of the output.
    """

    await_interval:float = 0.1
    await_maxtime:float = 30
    verbose:bool = True

    def verbose_print(self, *args, **kwargs):
        if self.verbose:
            print(*args, **kwargs)
        else:
            pass

    @property
    def dependency_dict(self):
        if not hasattr(self, "_nei"):
            self._dependency_dict = self.get_dependency_dict()
        return self._dependency_dict
    
    @property
    def deps(self):
        """shortcut for dependency_dict"""
        return self.dependency_dict

    def get_dependency_dict(self):
        """
        Get a 
        """
        nei = defaultdict(dict)
        for i, sg in enumerate(self._stages):
            sg = self._stages[i]
            func_args = set(reduce(lambda x, y: x + y, sg.funcs_args.values() ))
        
        
            for Ns, args in sg.funcs_args.items():
                nei[i][Ns] = set(args)
                # nei[(i, Ns)] = set(args)
                
                # append passing by value they are not in the previous stage
                if i > 0:
                    prev_output = set(sg.prev.funcs.keys())
                    pass_by_ns = func_args - prev_output
                    if len(pass_by_ns) > 0:
                        # print(f"Pass by {Ns}", pass_by_ns)
                        for ns in pass_by_ns:
                            nei[i-1][ns] = set([ns])

        return nei

    async def _invoke_async_(self, i, Ns):
        """ Internal Method to invoke a function asynchronously"""

        max_wait_itr = self.await_maxtime / self.await_interval

        deps = self.deps

        dep_satisified = [self.stages[i].input[dep] for dep in deps[i][Ns]]
        
        # as long as all dependency are not satisfied, wait
        while not all(dep_satisified):
            self.verbose_print(Fore.BLUE + "Waiting for dependencies for %s %s" % (i, Ns) + Fore.RESET)
            await asyncio.sleep(self.await_interval)
            # prevent iteration from infinitely runing
            max_wait_itr -= 1
            if max_wait_itr <= 0:
                self.verbose_print(Fore.RED + "Max wait reached for %s %s" % (i, Ns) + Fore.RESET)
                return None
        
        self.verbose_print(Fore.GREEN + "Invoking %s %s" % (i, Ns) + Fore.RESET)
        return self.stages[i].invoke(Ns)
    async def invoke_async(self):
        deps = self.deps
        async with asyncio.TaskGroup() as tg:
            for i in deps:
                for Ns in deps[i].keys():
                    tg.create_task(self._invoke_async_(i, Ns))
            self.verbose_print(Fore.GREEN + "All tasks created.." + Fore.RESET)