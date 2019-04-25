# Ambari Metrics Collector

Ambari Metrics Collector (AMCollector) is a Python collector which collects all metrics from Ambari Metrics.

AMCollector calls Ambari Metrics REST Api to get metrics and also get blacklist conf from black_list.yaml. 
From these, it create prometheus metrics.

## Usage

Start the collector by

```bash
python collector.py
```  

## Notes
- Collector support only numeric metric values, not yet supports enum types.

## Local debug
- Start the collector, it will listen on port <user-defined-port>
- Curl it

```bash
python collector.py
curl -v localhost:<user-defined-port>
``` 

