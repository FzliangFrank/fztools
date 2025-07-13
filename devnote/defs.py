import os
import time
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

def worker1():
    # printing process id
    print("ID of process running worker1: {}".format(os.getpid()))

def worker2():
    # printing process id
    print("ID of process running worker2: {}".format(os.getpid()))



# empty list with global scope 
result = [] 

def square_list(mylist): 
    """ 
    function to square a given list 
    """
    global result 
    # append squares of mylist to global list result 
    for num in mylist: 
        result.append(num * num) 
    # print global list result 
    print("Result(in process p1): {}".format(result)) 

 
def square_list2(mylist, result, square_sum): 
    """ 
    function to square a given list 
    """
    # append squares of mylist to result array 
    for idx, num in enumerate(mylist): 
        result[idx] = num * num 
  
    # square_sum value 
    square_sum.value = sum(result) 
  
    # print result Array 
    print("Result(in process p1): {}".format(result[:])) 
  
    # print square_sum Value 
    print("Sum of squares(in process p1): {}".format(square_sum.value)) 

 
def print_records(records): 
    """ 
    function to print record(tuples) in records(list) 
    """
    for record in records: 
        print("Name: {0}\nScore: {1}\n".format(record[0], record[1])) 
  
def insert_record(record, records): 
    """ 
    function to add a new record to records(list) 
    """
    records.append(record) 
    print("New record added!\n") 



# Queue
def square_list3(mylist, q): 
    """ 
    function to square a given list 
    """
    # append squares of mylist to queue 
    for num in mylist: 
        new_num = num * num
        print(f"Square of {num} is {new_num}")
        q.put(new_num) 
def print_queue(q): 
    """ 
    function to print queue elements 
    """
    print("Queue elements:") 
    while not q.empty(): 
        print(q.get()) 
    print("Queue is now empty!") 


# Pipe

def sender(conn, msgs): 
    """ 
    function to send messages to other end of pipe 
    """
    for msg in msgs: 
        conn.send(msg) 
        print("Sent the message: {}".format(msg)) 
    conn.close() 
  
def receiver(conn): 
    """ 
    function to print the messages received from other 
    end of pipe 
    """
    while 1: 
        msg = conn.recv() 
        if msg == "END": 
            break
        print("Received the message: {}".format(msg)) 

def long_process1():
    time.sleep(3)
    print('long_process1 done (taken 3 seconds)')
def long_process2():
    time.sleep(1)
    print('long_process2 done (taken 1 second)')