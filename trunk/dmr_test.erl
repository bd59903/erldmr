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
-export([db_load/1, db_count/1, db_avg_price/1, db_low_price/1]).
-export([db_inventory/1, db_inventory_value/1, db_buy/3]).
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
    lists:merge(dmr:map_reduce(
        fun (Num) -> {[Num]} end,
        fun (Results) -> [lists:sort(Results)] end)).

%
% database tests
%

% this will load X tuples of "random" data
% Fruit Schema is: {Id, Name, Price, Quantity, Grocer, Organic, Description}
db_load(X) ->
    db_load(X, 1,
        ["Blackcurrant", "Redcurrant", "Gooseberry", "Tomato", "Guava",
         "Lucuma", "Pomegranate", "Avocado", "Kiwifruit", "Grape", "Orange",
         "Lemon", "Lime", "Grapefruit", "Banana", "Cranberry", "Blueberry",
         "Blackberry", "Raspberry", "Boysenberry", "Pineapple", "Fig",
         "Mulberry", "Apple", "Apricot", "Peach", "Cherry", "Strawberry"],
        [1.99, 0.49, 0.79, 3.99, 1.49, 0.99, 2.49, 1.19, 2.99, 3.49, 0.89],
        [456, 87, 34, 877, 344, 23, 54, 677, 45, 11, 0, 776, 96],
        ["New Seasons", "People's Co-op", "Food Front", "Alberta Co-op",
         "Food Fight!", "Wild Oats", "Whole Foods", "Fred Meyer"],
        [true, false, true, true, false, true, false, false, true, false],
        ["These are quite tasty!", "This batch was a little bitter",
         "They are now overripe", "These are one the sweet side",
         "Purchased at the farmer's market", "This batch was full of bugs!",
         "Ick! This tasted horrible!", "These are from a Washington farmer",
         "These make for a great tasting pie or cobbler, yum!"]).

db_load(0, _, _, _, _, _, _, _) -> ok;
db_load(X, Id, [N | Name], [P | Price], [Q | Quantity], [G | Grocer],
    [O | Organic], [D | Description]) ->
    dmr:add_fast({Id, N, P, Q, G, O, D}),
    db_load(X - 1, Id + 1, Name ++ [N], Price ++ [P], Quantity ++ [Q],
        Grocer ++ [G], Organic ++ [O], Description ++ [D]).

% returns the total count of fruit with the given name
db_count(Name) ->
    lists:sum(dmr:map_reduce_args(
        fun ({_, N, _, Q, _, _, _}, N) -> {[Q]};
            (_, _) -> ok
        end,
        fun (Results, _) -> [lists:sum(Results)] end,
        Name)).

% get the average price for a type of fruit
db_avg_price(Name) ->
    avg(dmr:map_reduce_args(
        fun ({_, N, P, _, _, _, _}, N) -> {[P]};
            (_, _) -> ok
        end,
        fun (Results, _) -> [{length(Results), lists:sum(Results)}] end,
        Name)).

% get the lowest price, grocer, and quantity for a type of fruit
db_low_price(Name) ->
    case dmr:map_reduce_args(
        fun ({_, N, P, Q, G, _, _}, N) -> {[{P, G, Q}]};
            (_, _) -> ok
        end,
        fun ([], _) -> [];
            (Results, _) -> [lists:min(Results)] end,
        Name) of
        [] -> [];
        Results -> lists:min(Results)
    end.

% get a section of a sorted inventory list for a given grocer
db_inventory(Grocer) ->
    lists:merge(dmr:map_reduce_args(
        fun ({Id, N, P, Q, G, O, _}, G) -> {[{N, O, P, Id, Q}]};
            (_, _) -> ok
        end,
        fun (Results, _) -> [lists:sort(Results)] end,
        Grocer)).

% get the value of all inventory items for a given grocer
db_inventory_value(Grocer) ->
    lists:sum(dmr:map_reduce_args(
        fun ({_, _, P, Q, G, _, _}, G) -> {[P * Q]};
            (_, _) -> ok
        end,
        fun (Results, _) -> [lists:sum(Results)] end,
        Grocer)).

% buy some quantity of fruit a given from from grocer
db_buy(Name, Grocer, Quantity) ->
    lists:sum(dmr:map_reduce_args(
        fun ({Id, N, P, Q, G, O, D}, {N, G, BuyQ}) when BuyQ =< Q ->
                {[{Id, N, P, Q - BuyQ, G, O, D}], [P * BuyQ]};
            (_, _) -> ok
        end,
        fun (Results, _) -> [lists:sum(Results)] end,
        {Name, Grocer, Quantity})).

%
% helper functions
%

% get the average from aggregated {Count, Sum} pairs
avg(List) -> avg(List, 0, 0).
avg([], Count, Sum) -> Sum / Count;
avg([{C, S} | Tail], Count, Sum) -> avg(Tail, Count + C, Sum + S).

% make sure the given list is actually sorted
sort_verify([]) -> true;
sort_verify([H | T]) -> sort_verify(T, H).

sort_verify([], _) -> true;
sort_verify([H | T], Last) when Last =< H -> sort_verify(T, H);
sort_verify(_, _) -> false.
