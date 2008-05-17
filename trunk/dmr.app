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

{
    application,
    dmr,
    [
        {description, "Distributed Map/Reduce"},
        {vsn, "1.0"},
        {id, "dmr"},
        {modules, [dmr, dmr_counter, dmr_server, dmr_test]},
        {registered, [dmr_counter, dmr_server]},
        {applications, [kernel, stdlib]},
        {mod, {dmr, []}}
    ]
}.