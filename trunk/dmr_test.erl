-module(dmr_test).
-export([load/1, map/0]).

% this calls the distributed mapping function and returns all {id,num} pairs
% for apples, increments the apple number, and ignores all other tuples.
map() ->
    dmr:map(
        fun ({Id, "Apple", Tup, Num}) ->
                {[{Id, "Apple", Tup, Num + 1}], [{Id,Num}]};
            (Data) ->
                {[Data], []}
        end).

% this will just load X tuples of "random" data
load(X) ->
    load_record(X, 0, ["Apple", "Orange", "Banana"], [a,b,c,d,e,f,g],
        [12,54,665,746,3465]).

load_record(0, _, _, _, _) -> ok;
load_record(X, Id, [Name | Names], [Tup | Tups], [Num | Nums]) ->
    dmr:add({Id, Name, Tup, Num}),
    load_record(X - 1, Id + 1, Names ++ [Name], Tups ++ [Tup], Nums ++ [Num]).
