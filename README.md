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

# USING WITH STATSD
## Install required libs
```bash
pip install -r requirement.txt
```  

## Run local
Load environment. Read `dev.env` to know about environment variables
```bash
source env/dev.env
```  

Start the collector by

```bash
python collector.py
```  

## Run local statsd
- Document: https://github.com/hopsoft/docker-graphite-statsd
- Using Graphite UI to view statsd data at [port 81](http://localhost:81)
- Grafana run at [port 80](http://localhost)
```bash
docker run -d\
 --name graphite\
 --restart=always\
 -p 80:80\
 -p 81:81\
 -p 2003-2004:2003-2004\
 -p 2023-2024:2023-2024\
 -p 8125:8125/udp\
 -p 8126:8126\
 hopsoft/graphite-statsd
 ```

 ## Run in PROD
### 1. Create `prod.env`
 ```bash
cp env/sample.prod.env env/prod.env
```

### 2. Edit `prod.env`
```
export STATSD_HOST=localhost
export STATSD_PORT=8125
export STATSD_PREFIX=TELCOX
export STATSD_MAXUDPSIZE=512

export AMBARI_URI=https://localhost:8080
export AMBARI_USER=user
export AMBARI_PASS=pass

export BATCH_DELAY=60

export PYTHONWARNINGS="ignore:Unverified HTTPS request"
```

### 3. Run

```bash
source env/prod.env
python collector.py
``` 