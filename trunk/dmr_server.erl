-module(dmr_server).
-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_Args) ->
    pg2:create(dmr),
    pg2:join(dmr, self()),
    {ok, []}.

handle_call({add, Data}, _From, State) ->
    {reply, ok, [Data | State]};
handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast({map, {Pid, Func}}, State) ->
    {NewState, Results} = run_map(Func, State, [], []),
    Pid ! Results,
    {noreply, NewState};
handle_cast(_Msg, State) ->
    {noreply, State}.

run_map(_Func, [], NewState, Results) ->
    {NewState, Results};
run_map(Func, [Data | State], NewState, Results) ->
    {NewData, Result} = Func(Data),
    run_map(Func, State, NewState ++ NewData, Results ++ Result).

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
