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
## Integrate with Prometheus docker
- Pull the docker image prom/prometheus
```bash
docker run --name prom --init -p 9090:9090 -d prom/prometheus
```
- ssh into the docker container
```bash
docker ps -a
docker exec -it <container-pid> \bin\sh
```
- [Get the host ip](https://biancatamayo.me/blog/2017/11/03/docker-add-host-ip/)

For mac only using
```bash
ping docker.for.mac.localhost
```
- Change scrape_configs targets value in /etc/prometheus/prometheus.yml
- Restart the container



