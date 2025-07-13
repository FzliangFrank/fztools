
### Feature for Each Stack

**`stagemanager`**

The stagemanager should make it easy for you to debug and understand the dependency chain; what have changed in what stage?

A **special case** of stagemanager is when all the variables are "dataframe"/table/temporary table type; so that we can adding things such as table statistics into the stagemanager on each stage; 

Further we can create an object class with namespace to modify this;

- [x] Enable Different backend, stagemanager can take any object as input and as variable as output;
- [x] Dependency Diagram;
   - [ ] Fixing situation when name have quote mark;
   - [ ] Add a second dependency diagram using `class_dagram
- [ ] Paralel computing;
- [ ] Chache management intermediatory input:
   - Delete Object
   - Write in Memory (Copy do nothing)
   - Write in Desk (in a folder)? s

**`datapack`**

- [ ] Enable Different backend other than pandas
- [ ] Generic Class versus non-generic class
- [ ] Deep rename the summary column
- [ ] Reconsider the role of `_remove_` for capturing validation failure; maybe they need to be renamed to `_error_` or `__validation_failure__`
- [ ] Consider how to fix data based on particular failure (nameSpace, column, failure)

**`validation`**

- [ ] Enable Different backend other than pandas



### Development Resources

- [Multi-Processing](https://docs.python.org/2/library/multiprocessing.html)
