# Distributed Map/Reduce in Erlang #

**Eric Day**

**_http://code.google.com/p/erldmr/_**


---


### Abstract ###

This project was inspired by the MapReduce programming model developed at Google `[1]`. While the Google model focuses on large batch-style processing for massive amounts of data and returning analytical results, this lightweight and simplified implementation focuses more on everyday applications and how they can harness the same power the model provides. The current implementation serves as a prototype and leaves out some of the advanced features and refinements presented in the Google paper. Potential paths for development for this implementation are mentioned in section 6.

The model used here uses two core ideas provided by functional languages: map and reduce (or sometimes referred to as fold). Map is the act of executing (or "mapping") a given function over each item in a list. Reduce (or fold) is the act of collapsing a list of items into a smaller set (or single item) based on a given function. This paper explores a method of distributing these operations over a number of nodes, which may exist on separate physical machines. This provides enhanced performance through parallel execution. Along with retrieving information about individual items stored within each node, this implementation also supports the ability for the map function to mutate the stored items. Applications can use this in order to create dynamic data storage, which can then be extended to database oriented applications.

This implementation leverages many of the features functional languages provide to ease in deployment and testing. Treating functions as a first class object (or variable) is one of the key components to constructing quick tests and getting feedback since it allows you to define your function on the fly. You also have the built-in functions (bifs) and standard runtime modules within Erlang at your disposal when constructing your map and reduce functions. All code and supporting documentation for this project can be found at the URL given at the top of this paper. Basic Erlang knowledge is assumed throughout the paper, so please refer to the documentation from `[2]` if needed.

### 1 Introduction ###

While doing research on the various programming models for parallel and distributed computation systems, I began to think about ways that these ideas could be brought to more common applications. I was not able to find a simple system that could be setup in a short amount that would allow me to test these ideas and write some prototype applications. One specific application I had in mind was looking at ways to implement the functionality of a traditional database management system (DBMS), which is explored further in section 4.

This project aims to be a simple implementation of this distributed, parallel programming model that will allow people to get setup quickly so they can begin building their own prototypes without excessive overhead. Being based on a functional language brings all the benefits of functional language development such as fast development time, succinct code, and improved program correctness. Currently Erlang `[2]` is the only way to interface with the system, but the ability to interface with other languages is discussed as a possible improvement in section 6.5.

**1.1 Simplified Model**

This implementation focuses on the core of the programming model, and removes many of the robust and fault-tolerant features discussed in Google's implementation. This allowed for faster initial development, and these additional features may be added at a later time if needed.

One major simplification made in this implementation is not requiring an underlying global filesystem. The Google implementation is built upon their own file system (GFS) `[3]` which provides fast data access and fault-tolerance. The implementation described in this paper only deals with application level RAM-based storage, but persistent solutions are discussed in section 6.1. Not requiring a specific underlying filesystem allows for faster deployment due to less dependencies and the ability to start nodes on any system with just the Erlang runtime environment.

Another simplification is not supporting distribution of map and reduce functions on a particular partition of the data set. Both the map and reduce functions will be executed on the same node. While this requires more processing to be done on each node, it reduces the potential network latency for shuffling data between nodes for processing. Related to this is that there is no master node coordinating the map and reduce tasks across all nodes. This means if a particular node should fail the job will not be restarted. This is also something that can be added easily at a later time if needed.

![http://oddments.org/pics/dmr.png](http://oddments.org/pics/dmr.png)

**_Figure 1: Simplified Map/Reduce Model_**

**1.2 Functional Roots**

As mentioned before, the map and reduce function concept comes from the core set of functions for functional languages. Many other language constructs can built upon these. For example, in Erlang, the statement:

```
lists:map(fun (X) -> X * 2 end, [1,2,3,4,5]).
```

Will return the list `[2,4,6,8,10]`. As you can see, the function returns the value of the input returned by two, and this is applied to each element of the list. The reduce function, also commonly known as fold, takes a function, a list, and an initial value as argument to generate some final result. For example, to sum a list of numbers, you can use:

```
lists:foldr(fun (X, Sum) -> X + Sum end, 0, [1,2,3,4,5]).
```

This initializes `Sum` to 0, runs the given function on each element of the list (using the return value for the new `Sum` value), and when finished returns the value `15`. The idea of this project is to now take these basic building blocks and produce a system where the lists are very large, persistent, and spread over a number of nodes. The functions can then be run individually on the partitions on each node in parallel, eventually returning results to the calling process.

An important concept used here is the ability to pass entire functions as arguments, and not just references. Languages like C can only pass around function pointers, where functional languages pass the entire function within the variable. This idea is not unique to functional languages, but it is a necessary feature and is often referred to as a functional language concept. This is especially important when dealing with distributed nodes since the function may actually be passed over a network and remotely executed. Using this ability of functional languages makes development fast, since you can create them on the fly rather than having to push a code module to all nodes before being able to execute it.

**1.3 Why Erlang**

Erlang was natural choice to use for implementing this project for a number of reasons. Beyond being functional, Erlang was written to provide a robust, distributed runtime environment. Processes within Erlang can communicate with each other using a simple asynchronous message passing interface. The message passing is not restricted to processes within a single node either, they may be in another node, possibly on a separate physical machine. This provides an easy to use framework for building distributed applications.

Erlang also provides an easy way for distributed nodes to connect to one another and, once connected, for certain processes to create and join globally named process groups (using the pg2 module). I also leveraged some components of the Open Telecom Platform (OTP) to speed up the initial development of the server. By building on all of these existing components, only a couple hundred lines of code needed to be written to get the core functionality of this project working. Lastly, having been used in the telecommunications industry since it was written, we can be sure it is stable and has relatively good performance.

### 2 Implementation ###

This project follows the OTP design framework provided by the Erlang runtime system. More specifically, it uses the `application`, `supervisor`, and `gen_server` behaviors. Using these to design the core server allowed for a quick, robust client-server framework. Also, since these are common design principles in Erlang, other developers can come up to speed and understand the source quickly. The `dmr.app` file describes the runtime configuration parameters so a single node can be started by running:

```
erl -sname mynode -s dmr
```

This triggers the `application` behavior by running `dmr:start()` in `dmr.erl`. The supervisor behavior, which monitors and restarts processes under it, will then start and create two additional processes. One process will use `dmr_server.erl` which contains a request handler for the add, map and reduce requests. The other process will start a simple counter server using `dmr_counter.erl` to enable round-robin distribution when adding data.

**2.1 Client Interface**

The client interface is defined in `dmr.erl`. It consists of a an interface to add or delete data from the system, run a map function over the data in the system, or run a map and reduce function in the system. For each of the map and map\_reduce functions, you may specify an optional process ID to send results to, or an argument to pass into the map and reduce functions when called. There is also a stat function to grab the number of items held on each node in the system.

To add the integer `42` as a data item in the system, you would run:

```
dmr:add(42).
```

This will wait until the server process that handles the request to add the data and respond. If you simply want to send the request but not wait for a reply, you can use:

```
dmr:add_fast(43).
```

This will return immediately with no guarantee the server process completed. This function is much faster and is useful when bulk loading data for testing. To see how many items have been loaded into the system, you can run:

```
dmr:stat().
```

Which will return a list of `{node name,count}` pairs for each node in the system. Now, to run a map function on the data in the system, you can run:

```
dmr:map(fun (Num) -> {[Num + 1000]} end).
```

This will run a function which returns each item plus 1000 back to the calling process. The map function also has the ability to replace the existing values while returning a value by returning a two-tuple. The first item in the tuple is a list of new item values, and the second item in the tuple is the list of values to return to the calling process. For example:

```
dmr:map(fun (Num) -> {[Num + 100], [Num + 1000]} end).
```

This will return the same values as before (each item + 1000) but also replaces the existing value with the old value + 100. Now running the same map operation will return each value incremented by 100, and will also increment the items on the data nodes again by 100. Note that the replace and return items in the return tuple are lists, so you are able to give an empty list (which deletes the item or returns no items), or can even replace or return with multiple items. For example:

```
dmr:map(fun (Num) -> {[Num,Num], []} end)
```

This will duplicate all items in the system, which results in twice as many items overall. Also, since the return value is `[]` (the empty list), it will not return anything to the calling process.

The map\_reduce variants allow for a reduce function to be run on the resulting data set at each remote node before returning result to the calling process. This is especially useful for aggregate operations like summing up numbers. For example:

```
dmr:map_reduce(fun (Num) -> {[Num]} end,
               fun (Results) -> [lists:sum(Results)] end).
```

The map function will simply return the value, and then the reduce function will return a sum of values. This results in the calling process returning a list, where each item in the list is the sum of numbers from a particular node. This can then be passed into a `lists:sum()` in the calling process to obtain the overall sum of numbers in the system.

**2.2 Client Details**

A number of the client functions are built upon others to reduce the amount of core code. Most of the public functions construct request tuples and call one of `call_one`, `cast_one`, or `cast_all`. The `_one` variants, used by the add functions, grab one server process ID from a list associated with the globally registered process group name `dmr`. This process group represents all active server nodes than can handle requests. A single process is chosen in a round robin fashion using state kept in the `dmr_counter` server. The `cast_all` function, used by map and delete functions, will send the request to all process IDs associated with the global process group name. When using `cast_all`, there is also a `recv_all` call that can wait to receive a reply message from each process a request was sent to. This is used by the map function to gather results from each node, and will then pass the list of results back to the calling function.

**2.3 Server Interface**

The server interface consists of receiving messages through the message passing interface provided by the `gen_server` module. It allows for both asynchronous and synchronous calls to be passed using `gen_server:cast()` and `gen_server:call` respectively. The message handlers are defined as `handle_cast` and `handle_call` in the `dmr_server.erl` file. The messages supported consist of tuples specifying the desired operation and any arguments. It currently supports:

```
{add, Data}
{map, From, Map}
{map, From, Args, Map}
{map_reduce, From, Map, Reduce}
{map_reduce, From, Args, Map, Reduce}
```

The `add` operation adds the item `Data` to the data set on the receiving node. The `map` operation runs the given `Map` function on each data item on the node, with the optional `Args` parameter if given. The results will be returned directly. The `map_reduce` operator works just like `map`, but will also run the results of `map` through the given `Reduce` function before sending the data back to the calling process. All other message are simply ignored.

**2.4 Server Details**

There is a `State` variable associated with each `gen_server` instance, and this is currently the list of data items held for each node. The `add` operator will add a given `Data` item to this state list by returning `[Data | State]` as the new state value.

The `map` and `map_reduce` operators use the state variable for their input. They both call an instance of the `run_map` function to iterate over each item in the `State` list, possibly removing the item or adding to it (depending on the return tuple of the `Map` function). The `run_map` function expects one of two possible returns values from the given `Map` function. If the value is a single element tuple, the list is concatenated to the result list. If a two element tuple is returned, the first list is concatenated with the old `State` replacing the existing value, and the second list is again the return value for the calling process. Any other return value from `Map` is ignored, and neither the data set nor return list are modified.

The new data set and list of results accumulated throughout the `run_map` execution is eventually returned. The final result list is then run through the `Reduce` function if given. The return value of the `handle_cast` and `handle_call` calls indicate a reply (if any) and the new `State` variable representing the data set for the node.

### 3 Testing: Numerical Computation ###

In this section we will look at a few simple numerical applications to get a better idea of how the system works. This should also show how traditional algorithms need to be modified to work within this programming model. We begin by first starting up a four-node system by using the supplied start script and loading the system with one million numbers.

```
# ./start
Erlang (BEAM) emulator version 5.5.1
Eshell V5.5.1  (abort with ^G)
(d@host)1> dmr_test:num_load(1000000).
ok
(d@host)2> dmr:stat().
[{a@host,250000},{b@host,250000},{c@host,250000},{d@host,250000}]
```

The `dmr_test:num_load` function will actually load the data, and the second `dmr:stat` call returns how many data items are loaded on each node. As you can see there are four nodes running, each with an equal partition of the data due to the round-robin data loading. To increase the number of integers in the system and get some slightly more interesting values, we now run a custom map function to double the number and add some random offsets:

```
(d@host)3> dmr:map(fun (X) -> {[X + 8475, X + 42], []} end).
[]
(d@host)4> dmr:stat().                                      
[{d@host,500000},{b@host,500000},{c@host,500000},{a@host,500000}]
```

The supplied map function returned two new data items to replace the single given item, each with a different integer value added to them. The second stat call verifies the number of items did indeed double. To get the sum of all numbers within the system, we run:

```
num_sum() ->
    lists:sum(dmr:map_reduce(
        fun (Num) -> {[Num]} end,
        fun (Results) -> [lists:sum(Results)] end)).
```

This function is defined in the `dmr_test` module. The `num_avg` and `num_avg_sqrt` functions, which are defined in a similar way to `num_sum`, are also defined in this module. These functions return the average of all the numbers, and the average of the square root of all numbers, respectively. When run, we get the expected return values:

```
(d@host)5> dmr_test:num_sum().
1008518000000
(d@host)6> dmr_test:num_avg().
5.04259e+5
(d@host)7> dmr_test:num_avg_sqrt().
670.674
```

These functions demonstrate how much of the computation can happen local to where the data resides through the supplied reduce function. The calling process receives back only the required information to finish the calculation, such as the sum of each node, and then all those values are summed to get the total system sum. The `num_avg` and `num_avg_sqrt` functions work in a similar way.

Sorting presents a different type of problem since this requires returning a large data set to the calling process. Most of the heavy computation can still be distributed to each of the nodes by using a variant of the merge sort algorithm. Each remote data node will perform a local sort of the data items it holds and then return this sorted list to the calling process. The calling process can then perform the last step of the merge sort by repeatedly taking the lowest value from the head of each returned list and returning the final sorted list. The `dmr_test:num_sort` function demonstrates this:

```
(d@host)8> dmr_test:num_sort().
[43, 44, 45, ...]
(d@host)9> dmr_test:sort_verify(dmr_test:num_sort()).
true
```

It may be useful to look at the code for `dmr_test:num_sort` and the Erlang documentation for `lists:merge` to fully understand the implementation. The supplied `dmr_test:sort_verify` function will take a list of numbers and verify if it sorted or not. Simple verification functions such as this are useful when testing with large data sets.

### 4 Testing: Database Application ###

In this section we will explore a more complex example by building a simple database application. Rather than storing simple integers as the data items, we will be storing tuples which represent rows in a traditional database. Rather than using a query language such as SQL, we will be hand crafting our queries with map and reduce functions. The `dmr_test` file contains the load function for the sample data as well as a number of queries we can make on that data. The schema we will be looking at will be:

_Fruit Inventory Table_

| Id | Name | Price | Quantity | Grocer | Organic | Description |
|:---|:-----|:------|:---------|:-------|:--------|:------------|

We begin by loading ten thousand entries of some sample data using `dmr_test:db_load(10000)`.

```
# ./start
Erlang (BEAM) emulator version 5.5.1
Eshell V5.5.1  (abort with ^G)
(d@host)1> dmr_test:db_load(10000).
ok
(d@host)2> dmr:stat().
[{c@host,2500},{b@host,2500},{a@host,2500},{d@host,2500}]
```

With the data loaded, we experiment with some sample queries. In order to get an inventory count of a particular kind of fruit we match on a given name and return the quantity. To make the query interface somewhat dynamic, we define `dmr_test:db_count` to take the name and return the sum of the `Quantity` fields in the system matching that name. The code to do this is:

```
db_count(Name) ->
    lists:sum(dmr:map_reduce_args(
        fun ({_, N, _, Q, _, _, _}, N) -> {[Q]};
            (_, _) -> ok
        end,
        fun (Results, _) -> [lists:sum(Results)] end,
        Name)).
```

This is the first example of `dmr:map_reduce_args`, and we use this to pass the Name as the argument which provides a key for the function pattern matching. The given map function looks at the `Name` and `Quantity` fields of the current data item, and if the name is equal to the argument (note they are both `N`), then the quantity is returned. We require the second clause so that the map function always matches the data entry (in this case it doesn't change or return anything). The reduce function will then sum all the quantities on each data node and return a single count to the calling process. The calling process then sums the counts from each data node. We now run this along with some similarly defined functions:

```
(d@host)3> dmr_test:db_count("Apple").
95720
(d@host)4> dmr_test:db_avg_price("Apple").
1.89000
(d@host)5> dmr_test:db_low_price("Apple").
{0.490000,"Alberta Co-op",0}
```

The first query gets an inventory count of all the apples in all stores. The Second query retrieves the average price of all apples, and the third query will return the price, location, and quantity of the lowest priced apple. The code for the latter functions can also be found in the `dmr_test` module.

We can also do sorting queries much like we did in the numerical example. This is shown with the `dmr_test:db_inventory` function by projecting out the interesting fields from the item in the map function, sorting this in the reduce function, and then merging those sorted lists from each data node in the calling process. This will give a complete, sorted inventory list for a given grocer name.

```
(d@host)6> dmr_test:sort_verify(
               dmr_test:db_inventory("New Seasons")).     
true
(d@host)7> lists:sublist(
               dmr_test:db_inventory("New Seasons"), 178, 4). 
[{"Apricot",true,3.99000,8089,34},
 {"Apricot",true,3.99000,9321,96},
 {"Blackcurrant",false,0.490000,57,344},
 {"Blackcurrant",false,0.490000,1905,54}]
(d@host)8> dmr_test:db_inventory_value("New Seasons").                     
6.31839e+5
```

The first call retrieves the entire inventory list for "New Seasons" and verifies the entries are indeed sorted correctly. The second call retrieves the same inventory list, but this time only returns entries 178 through 181 to show some sample output. The last function calculates the total value of all items in the inventory for a given grocer. The underlying map function returns the `Price` multiplied by the `Quantity`, and then the reduce and calling process sum up the totals.

One more example to consider is when the data items need to be modified. We define a `dmr_test:db_buy` function that will purchase a given quantity of fruit from a grocer if they have them in stock. If there are enough in stock, the quantity is modified to reflect the purchase, and then total price is returned.

```
(d@host)9> dmr_test:db_buy("Apricot", "New Seasons", 10).              
3124.50
(d@host)10> lists:sublist(
                dmr_test:db_inventory("New Seasons"), 178, 4).
[{"Apricot",true,3.99000,8089,24},
 {"Apricot",true,3.99000,9321,86},
 {"Blackcurrant",false,0.490000,57,344},
 {"Blackcurrant",false,0.490000,1905,54}]
```

Note that this function will purchase 10 apricots from each row that matches the given name and grocer (otherwise that would be a lot to pay for 10 apricots!). The second query shows the same function as in the last example, but this time you can see the quantity field (the last one) is 10 less for each of the two apricot entries. This demonstrates a powerful idea where you are able to search, modify, and return some computed value all in one operation.

There are many other operations within a database management system that are not demonstrated here for brevity, but may still be possible. For example, a grouping operator could be done with a simple customer aggregator function. More advanced features like joins would require secondary messages to be passed among the data nodes. For example, the left input of a join may search for all matching tuples in the list, and then the result list could be sent off to data nodes holding entries for the right input. The matching could then be done on each of those nodes and the final result sent to the calling process. This type of message passing is currently not supported, but could be with some minor modifications.

There are also synchronization and atomicity issues to deal with, but this may not be important depending on the application. When these issues are important, there are a number of solutions out there for dealing with these issues such as global locking systems. New solutions for this particular kind of programming model may also be possible. The purpose of this section is to present some basic examples and operators that complex systems can be built on.

### 5 Performance ###

To understand the performance benefits that this storage and query model offers, we now use some of the example functions above to test the system. These queries are run on three different node configurations: one node running on one machine, four nodes running on one machine, and four nodes each running on three machines (12 total nodes). The machines used to perform these tests are running Linux, each have two quad-core processors, and have enough RAM to hold the entire data sets so no disk swapping is encountered. Each machine is connected in a LAN using 100MB ethernet so latency is negligible. Even thought the machines were idle during testing, multiple executions of each query type were run to filter out any anomalies and unexpected system load. All times are shown in milliseconds.

**5.1 Numerical Tests**

For the numerical tests there were 20 million entries loaded into each system configuration. The following functions defined in `dmr_test` were run:

| Query | 1-Node | 4-Node | 3X4-Node |
|:------|:-------|:-------|:---------|
| num\_sum | 7814   | 1701   | 638      |
| num\_avg\_sqrt | 11070  | 2277   | 840      |
| num\_sort | 7421   | 5080   | 7574     |

**_Table 1: Numerical query times (in ms) for each node configuration_**

As expected, the more computation heavy processes ran much faster with an increased number of nodes. The multi-node results for `num_sort` are not surprising since each data node must send a large list of sorted integers back to the calling process. This yields a faster time in the 4-Node configuration, but once the nodes span a physical network, the network transfer times become significant. While the query may be slower overall, the computation cost of sorting is still distributed among all nodes.

**5.2 Database Tests**

For the database tests there were two million entries loaded into each system configuration. The following functions defined in `dmr_test` were run:

| Query | 1-Node | 4-Node | 3X4-Node |
|:------|:-------|:-------|:---------|
| db\_count | 423    | 172    | 69       |
| db\_inventory | 5115   | 4014   | 1649     |
| db\_buy | 9362   | 3602   | 118      |

**_Table 2: Database query times (in ms) for each node configuration_**

Again, we see significant improvements in query times as more nodes are added. In this case, the `db_inventory` query time still improves when the nodes span physical machines. This is most likely due to the fact that the data set being returned is smaller than the data in the numerical example (there are less overall data items and the query is selective).

**5.3 Larger Node Sets**

It is important to mention that this type of scalability will eventually hit some limit. As the Google MapReduce paper explains, a significant amount of time is spent starting up jobs on the system when using 1,800 nodes. The system described in this paper would most likely suffer from the same type of startup delay if run on that large of a node set. To mitigate these issues, the nodes could be configured with another level of node groups so that the calling process would initially send the request to a number of relay processes, each of which is connected to a separate group of nodes. This tree structure would prevent a single process from having to coordinate with too many data nodes. If an application has multiple types of data sets, it could also choose to only use a certain set of nodes for each data set type. Many other types of node grouping and data partitioning could be used to accommodate a large number of nodes.

### 6 Improvements ###

This section briefly discusses potential improvements that can be made to the system. Since this is meant to be an implantation for prototyping and exploring ideas, some of these may not be worth the time required to implement. They are primarily mentioned as topics to keep in mind if a production quality system is designed and implemented on this model.

**6.1 Persistent Storage**

One major weak point in the system is the lack of persistent storage. Currently if a node stops or crashes for any reason all data that it holds is lost. A more robust implementation would use a disk-backed storage mechanism so data may be accessible when restarted. The data could be stored as a flat file or in some other database structure. It may be useful to provide a simple indexing mechanism within the system and store data in B+ trees on disk. This would prevent requiring the entire data set to be scanned for each query. To implement persistent storage in Erlang, one might look at the `dets`, `gb_trees`, or `mnesia` modules.

**6.2 Redundancy**

If the data being stored within the system is critical, some level of redundancy will be required. The replication model may be single or multi master, have certain restraints due to the storage mechanism, or have restrictions due to geographically distributed nodes. Many sources discuss these options in detail so no information will be given here. If the system is being designed in Erlang, one solution to consider is to leverage `mnesia` for a storage method since it has built-in data replication.

**6.3 Access Control Lists**

The current system has no authentication or any kind of access control, but this is required if used in a shared application environment. A simple solution is to identify each data item with an authentication ID and require queries to provide credentials. It may also be useful to implement some type of group system on top of this so data items may be read by multiple authentication IDs. One other solution is to give each authentication ID their own namespace and separate physical data set.

**6.4 External Interfaces**

Another limiting aspect of the current implementation is the lack of external interfaces. Only Erlang processes attached to one of the data nodes can currently access the data sets. Since the current interface requires Erlang functions, some level of abstraction would be required here. An interface that accepts strings which are then compiled into code could be used as arguments for the map and reduce functions. Queries could then be accepted through a TCP server or Erlang port and would make bindings to other languages possible. This straightforward implementation would require embedded Erlang functions in other language code. Another option is to introduce a new query language that can be compiled and run during execution.

### 7 Conclusion ###

With processing power spreading to multiple cores and larger clusters of machines, concurrent and distributed systems are playing a key role in developing ways to solve problems. The ideas and programming model presented in this paper should help provide a testing and prototype framework to build these types of systems on. It is my hope that further development will be continue and a more robust, production quality system can be based on the work presented.


---


### References ###

`[1]` Jeffrey Dean and Sanjay Ghemawat. MapReduce: Simplified Data Processing on Large Clusters. http://labs.google.com/papers/mapreduce.html.

`[2]` Erlang. http://erlang.org/.

`[3]` Sanjay Ghemawat, Howard Gobioff, and Shun-Tak Leung. The Google File System. http://labs.google.com/papers/gfs.html.