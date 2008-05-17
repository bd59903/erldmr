-module(dmr).
-behaviour(application).
-behaviour(supervisor).

-export([start/0, stop/0, restart/0, start/2, stop/1, init/1]).
-export([add/1, map/1, map/2]).

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

% external interface and helper functions

add(Data) ->
    gen_server:call(get_pid(), {add, Data}).

map(Func) ->
    map(Func, self()).

map(Func, From) ->
    Nodes = get_pid_list(),
    cast_all(Func, From, Nodes),
    recv_all(length(Nodes), []).

% mapred(Map, Red, From) ->  reduce on remote node

cast_all(_Func, _From, []) ->
    [];
cast_all(Func, From, [Pid | Pids]) ->
    gen_server:cast(Pid, {map, {From, Func}}),
    cast_all(Func, From, Pids).

recv_all(0, Results) -> Results;
recv_all(Total, Results) ->
  receive
    Result ->
      recv_all(Total - 1, Result ++ Results)
  end.

get_pid() ->
    Count = gen_server:call(dmr_counter, get),
    Nodes = get_pid_list(),
    lists:nth(1 + (Count rem length(Nodes)), Nodes).

get_pid_list() ->
    pg2:get_members(dmr).
