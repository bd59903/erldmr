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
-export([all/0, count/0, count_where/0, count_num/0, select/0, load/1]).

% run all tests
all() ->
    [
        {count, count()},
        {count_where, count_where()},
        {count_num, count_num()}
    ].

% get a count of all objects in system
count() ->
    dmr:map_reduce(
        fun (_) ->
            {[1]}
        end,
        fun (Results) ->
            [length(Results)]
        end).

% get a count of all objects in system, grouped by source node
count_where() ->
    dmr:map_reduce(
        fun (_) ->
            {[1]}
        end,
        fun (Results) ->
            [{node, length(Results)}]
        end).

% sum all of the 'Num' elements of the tuples
count_num() ->
    dmr:map_reduce(
        fun ({_, _, _, Num}) ->
                {[Num]};
            (_) ->
                ok
        end,
        fun (Results) ->
            [lists:sum(Results)]
        end).

% returns all {id,num} pairs for apples and increment the apple number
select() ->
    dmr:map(
        fun ({Id, "Apple", Tup, Num}) ->
                {[{Id, "Apple", Tup, Num + 1}], [{Id,Num}]};
            (_) ->
                ok
        end).

% this will load X tuples of "random" data
load(X) ->
    load_record(X, 0, ["Apple", "Orange", "Banana"], [a,b,c,d,e,f,g],
        [12,54,665,746,3465]).

load_record(0, _, _, _, _) -> ok;
load_record(X, Id, [Name | Names], [Tup | Tups], [Num | Nums]) ->
    dmr:add_fast({Id, Name, Tup, Num}),
    load_record(X - 1, Id + 1, Names ++ [Name], Tups ++ [Tup], Nums ++ [Num]).
