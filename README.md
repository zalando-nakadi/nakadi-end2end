Measuring time for message end-to-end processing
================================================

Application developed to measure request end to end processing time for nakadi
messaging bus.
Measuring is made once in a configured amount of time, two different metrics are
collected:
 - time for getting result in a single thread, with full consumer initialization
 - time for getting result using pre-configured consumer.

Usage
-----
Metrics are exposed on /metrics interface, and by itself are json object with 
folowing structure:

```
{
    "RPS": {
        "count": 39,
        "m15": 0.4521719763291768,
        "m5": 0.9613705309612733,
        "m1": 0.8205695562490346,
        "last": 0
    },
    "connector": {
        "ConnectorName": {
            "sync": {
                "count": 16,
                "m15": 1.6057883180980432,
                "m5": 1.700035727845394,
                "m1": 1.6746732757578584,
                "last": 0.3992905616760254
            },
            "send": {
                "count": 17,
                "m15": 0.1024359547067334,
                "m5": 0.1271120699292804,
                "m1": 0.15988687175979602,
                "last": 0.10791873931884766
            },
            "async": {
                "count": 17,
                "m15": 0.9287806114047278,
                "m5": 0.7207024849508458,
                "m1": 0.2745537887188304,
                "last": 0.16915297508239746
            },
            "async_max": {
                "count": 17,
                "m15": 4.536990908343303,
                "m5": 3.419018796004538,
                "m1": 0.9943996174992057,
                "last": 0.5673809051513672
            },
            "publish": {
                "status_200": 17
            },
            "rps": {
                "count": 33,
                "m15": 0.08120957438943488,
                "m5": 0.2049546226933957,
                "m1": 0.4458837237824493,
                "last" : 0
            }
        }
    }
}
```
Where:
 - `ConnectorName` - name of configured connector
 - `async` - time of end2end processing using preconfigured measurer (first one, if there are many of them)
 - `async_max` - time of end2end processing using preconfigured measurer (last one, if there are many of them)
 - `sync` - time of end2end processing using full initilization of consumer
 - `m1` - 1 minute average of processing time
 - `m5` - 5 minute average of processing time
 - `m15` - 15 minute average of processing time
 - `RPS` - total end2end RPS
 - `rps` - per connector RPS

Configuration
-------------
Configuration could be received on GET /connectors interface with the following structure:
```
{
    nakadi_e2e_512: {
        verify: false,
        trash-size: 512,
        type: "nakadi",
        interval: 2,
        topic: "end2end_monitor.test_invoked_512",
        receivers: 20,
        host: "https://nakadi-sandbox.aruha-test.zalan.do"
    }
}
```

To change configuration on can use `POST /connectors`, which replaces all the configuration of connectors.
 Old connectors are fully deleted, new ones are created (yep, it's not the REST way, but it's simpler).
 Configuration format is the same as returned on `GET /connectors`