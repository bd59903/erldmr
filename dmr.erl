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

-module(dmr).
-behaviour(application).
-behaviour(supervisor).

-export([start/0, stop/0, restart/0, start/2, stop/1, init/1]).
-export([add/1, add_fast/1, map/1, map/2, map_args/2, map_args/3]).
-export([map_reduce/2, map_reduce/3, map_reduce_args/3, map_reduce_args/4]).

-define(MAX_RESTART, 5).
-define(MAX_TIME, 60).

% application and supervisor related functions

start() ->
    net_adm:world(),
    application:start(?MODULE).

stop() ->
    application:stop(?MODULE).

restart() ->
    stop(),
    start().

start(_Type, _Args) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
    ok.

init(_Args) ->
    SupFlags = {one_for_one, ?MAX_RESTART, ?MAX_TIME},
    ServerSpec = childspec(dmr_server),
    CounterSpec = childspec(dmr_counter),
    {ok, {SupFlags, [ServerSpec, CounterSpec]}}.

childspec(dmr_server) ->
    {
        dmr_server,
        {dmr_server, start_link, []},
        permanent,
        brutal_kill,
        worker,
        [dmr_server]
    };
childspec(dmr_counter) ->
    {
        dmr_counter,
        {dmr_counter, start_link, []},
        permanent,
        brutal_kill,
        worker,
        [dmr_counter]
    }.

% external interface and related helper functions

add(Data) ->
    call_one({add, Data}).

add_fast(Data) ->
    cast_one({add, Data}).

map(Map) ->
    map(Map, self()).

map(Map, From) ->
    map_proc({map, From, Map}).

map_args(Map, Args) ->
    map_args(Map, Args, self()).

map_args(Map, Args, From) ->
    map_proc({map, From, Args, Map}).

map_reduce(Map, Reduce) ->
    map_reduce(Map, Reduce, self()).

map_reduce(Map, Reduce, From) ->
    map_proc({map_reduce, From, Map, Reduce}).

map_reduce_args(Map, Reduce, Args) ->
    map_reduce_args(Map, Reduce, Args, self()).

map_reduce_args(Map, Reduce, Args, From) ->
    map_proc({map_reduce, From, Args, Map, Reduce}).

map_proc(Msg) ->
    Pids = get_pid_list(),
    cast_all(Msg, Pids),
    recv_all(length(Pids), []).

call_one(Msg) ->
    gen_server:call(get_pid(), Msg).

cast_one(Msg) ->
    gen_server:cast(get_pid(), Msg).

cast_all(_Msg, []) ->
    [];
cast_all(Msg, [Pid | Pids]) ->
    gen_server:cast(Pid, Msg),
    cast_all(Msg, Pids).

recv_all(0, Results) -> Results;
recv_all(Total, Results) ->
  receive
    Result ->
      recv_all(Total - 1, Result ++ Results)
  end.

get_pid() ->
    Count = gen_server:call(dmr_counter, get),
    Pids = get_pid_list(),
    lists:nth(1 + (Count rem length(Pids)), Pids).

get_pid_list() ->
    pg2:get_members(dmr).
