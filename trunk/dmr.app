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
