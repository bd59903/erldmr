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
