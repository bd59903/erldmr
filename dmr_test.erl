%%
%% Erlang Distributed Map/Reduce
%% Copyright (C) 2008 Eric Day
%%
%% This program is free software; you can redistribute it and/or
%% modify it under the terms of the GNU General Public License
%% as published by the Free Software Foundation; either version 2
%% of the License, or (at your option) any later version.
%%
%% This program is distributed in the hope that it will be useful,
%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
%% GNU General Public License for more details.
%%
%% You should have received a copy of the GNU General Public License
%% along with this program; if not, write to:
%% Free Software Foundation, Inc.
%% 51 Franklin Street, Fifth Floor,
%% Boston, MA  02110-1301, USA
%%

-module(dmr_test).
-export([num_load/1, num_sum/0, num_avg/0, num_avg_sqrt/0, num_sort/0]).
-export([db_load/1, db_select/0]).
-export([sort_verify/1]).

%
% numerical tests
%

% load X numbers
num_load(Num) ->
    lists:map(fun dmr:add_fast/1, lists:seq(1, Num)),
    ok.

% get the sum of all numbers
num_sum() ->
    lists:sum(dmr:map_reduce(
        fun (Num) -> {[Num]} end,
        fun (Results) -> [lists:sum(Results)] end)).

% get the average of all numbers
num_avg() ->
    avg(dmr:map_reduce(
        fun (Num) -> {[Num]} end,
        fun (Results) -> [{length(Results), lists:sum(Results)}] end)).

% get the average of square root of all numbers
num_avg_sqrt() ->
    avg(dmr:map_reduce(
        fun (Num) -> {[math:sqrt(Num)]} end,
        fun (Results) -> [{length(Results), lists:sum(Results)}] end)).

% perform a distributed merge sort on all numbers
num_sort() ->
    merge_sort(dmr:map_reduce(
        fun (Num) -> {[Num]} end,
        fun (Results) -> [lists:sort(Results)] end)).

%
% database tests
%

% this will load X tuples of "random" data
db_load(X) ->
    db_load(X, 0,
        ["Apple", "Orange", "Banana"],
        [a,b,c,d,e,f,g],
        [12,54,665,746,3465]).

db_load(0, _, _, _, _) -> ok;
db_load(X, Id, [Name | Names], [Tup | Tups], [Num | Nums]) ->
    dmr:add_fast({Id, Name, Tup, Num}),
    db_load(X - 1, Id + 1, Names ++ [Name], Tups ++ [Tup], Nums ++ [Num]).

% returns all {id,num} pairs for apples and increment the apple number
db_select() ->
    dmr:map(
        fun ({Id, "Apple", Tup, Num}) ->
                {[{Id, "Apple", Tup, Num + 1}], [{Id,Num}]};
            (_) ->
                ok
        end).

%
% helper functions
%

% get the average from aggregated {Count, Sum} pairs
avg(List) -> avg(List, 0, 0).
avg([], Count, Sum) -> Sum / Count;
avg([{C, S} | Tail], Count, Sum) -> avg(Tail, Count + C, Sum + S).

% sort 
merge_sort([]) -> [];
merge_sort([[]]) -> [];
merge_sort(Results) ->
    {Next, NewResults} = merge_sort_next(Results),
    [Next | merge_sort(NewResults)].

merge_sort_next([[] | Results]) ->
    merge_sort_next(Results);
merge_sort_next([[H | T] | Results]) ->
    merge_sort_next(Results, H, T, []).

merge_sort_next([], Next, NextT, NewResults) ->
    {Next, [NextT | NewResults]};
merge_sort_next([[H | T] | Results], Next, NextT, NewResults) when H < Next ->
    merge_sort_next(Results, H, T, [[Next | NextT] | NewResults]);
merge_sort_next([Skip | Results], Next, NextT, NewResults) ->
    merge_sort_next(Results, Next, NextT, [Skip | NewResults]).

sort_verify([]) -> true;
sort_verify([H | T]) -> sort_verify(T, H).

sort_verify([], _) -> true;
sort_verify([H | T], Last) when Last =< H -> sort_verify(T, H);
sort_verify(_, _) -> false.
