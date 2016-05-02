Measuring time for message end-to-end processing
================================================

Application developed to measure request end to end processing time for nakadi
messaging bus.
Measuring is made once in a configured amount of time, two different metrics are
collected:
 - time for getting result in a single thread, with full consumer initialization
 - time for getting result using pre-configured consumer.

Metrics are exposed on /metrics interface, and by itself are json object with 
folowing structure:

```
{
    "metric_1": {
        "pre_configured": {
            "m1": 0.4,
            "m5": 0.6,
            "m15": 1.0
        },
        "full_init": {
            "m1": 0.4,
            "m5": 0.6,
            "m15": 1.0
        }
    },
    "metric_N": {
        "pre_configured": {
            "m1": 0.4,
            "m5": 0.6,
            "m15": 1.0
        },
        "full_init": {
            "m1": 0.4,
            "m5": 0.6,
            "m15": 1.0
        }
    }
}
```
Where:
 - `pre_configured` - time of end2end processing using preconfigured measurer
 - `full_init` - time of end2end processing using full initilization of consumer
 - `m1` - 1 minute average of processing
 - `m5` - 5 minute average of processing
 - `m15` - 15 minute average of processing

