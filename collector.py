import os
import json
import yaml
import re
import time
import logging
# from prometheus_client import start_http_server
# from prometheus_client.core import GaugeMetricFamily, REGISTRY
import requests

LOGLEVEL = os.getenv('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s'
    , datefmt='%m/%d/%Y %I:%M:%S %p'
    , level=LOGLEVEL
)

try:
    from statsd.defaults.env import statsd
except Exception as e:
    logging.error(e)

DUMMY_INPUT_FILE_NN = os.getenv('DUMMY_INPUT_FILE_NN', 'dummy_data/nn_jmx.json')
DUMMY_INPUT_FILE_RM = os.getenv('DUMMY_INPUT_FILE_RM', 'dummy_data/rm_metrics.json')
DUMMY_INPUT_FILE_ALERT = os.getenv('DUMMY_INPUT_FILE_ALERT', 'dummy_data/alerts_summary.json')
BATCH_DELAY = os.getenv('BATCH_DELAY', 60)
CONF_FILE = os.getenv('CONF_FILE', 'conf/components.yaml')


class AmbariMetricCollector(object):
    def __init__(self, conf_file):
        # self.prom_metrics = {}
        self.statsd_metrics = {}
        self.ambari_info = {
            'AMBARI_URI': os.getenv('AMBARI_URI')
            , 'AMBARI_USER': os.getenv('AMBARI_USER')
            , 'AMBARI_PASS': os.getenv('AMBARI_PASS')
        }

        with open(conf_file, "r") as read_conf_file:
            conf = yaml.load(read_conf_file)

        self.conf_nn, self.conf_rm, self.conf_alert = self._parse_conf(conf)

    def _parse_conf(self, conf):
        conf_nn = conf.get('namenode', None)
        conf_rm = conf.get('resourcemanager', None)
        conf_alert = conf.get('ambari_alert', None)

        return conf_nn, conf_rm, conf_alert

    def debug_conf(self):
        print self.conf_nn
        print self.conf_rm
        print self.conf_alert

    def _parse_metrics(self, data, prefix, metrics):
        for k, v in data.items():
            if type(v) is dict:
                prefix.append(k)
                self._parse_metrics(v, prefix, metrics)
                if len(prefix) > 0:
                    prefix.pop(len(prefix) - 1)
            else:
                # metric_name = '{}_{}'.format('_'.join(prefix), k)
                metric_name = '{}.{}'.format('.'.join(prefix), k)

                if type(v) is int or type(v) is float:
                    metrics[metric_name] = v

    def _parse_with_filter(self, data, prefix, metrics, conf):
        new_metrics = {}
        self._parse_metrics(data, prefix, new_metrics)

        if conf.get('white_list', None):
            self._filter_metric_in_white_list(new_metrics, conf.get('white_list', None))
        elif conf.get('black_list', None):
            self._filter_metric_in_black_list(new_metrics, conf.get('black_list', None))

        metrics.update(new_metrics)

    def _filter_by_rule(self, metrics, rules, is_wl=True):
        if rules is None:
            return

        remove_metrics = []
        for k, v in metrics.items():
            if is_wl:
                remove = True
                for bl in rules:
                    if re.search(bl, k):
                        remove = False
                        break

                if remove:
                    remove_metrics.append(k)
            else:
                for bl in rules:
                    if re.search(bl, k):
                        remove_metrics.append(k)
                        break

        for rm in remove_metrics:
            if rm in metrics:
                del metrics[rm]

    def _filter_metric_in_black_list(self, metrics, metrics_bl=None):
        self._filter_by_rule(metrics, metrics_bl, is_wl=False)

    def _filter_metric_in_white_list(self, metrics, metrics_wl=None):
        self._filter_by_rule(metrics, metrics_wl, is_wl=True)

    def _collect_ambari_alerts(self, metrics, dummy):
        if dummy:
            with open(DUMMY_INPUT_FILE_ALERT, "r") as read_metric_file:
                data = json.load(read_metric_file)
        else:
            try:
                data = self._call_ambari_api(self.conf_alert['url'])
            except Exception as e:
                logging.error('Call Ambari API error.\n {}'.format(e))

                return

        self._parse_with_filter(data['alerts_summary'], ['ambari_alert'], metrics, self.conf_alert)

    def _collect_namenode_metrics(self, metrics, dummy):
        if dummy:
            with open(DUMMY_INPUT_FILE_NN, "r") as read_metric_file:
                data = json.load(read_metric_file)
        else:
            try:
                data = self._call_json_api(self.conf_nn['url'])
            except Exception as e:
                logging.error('Call Namenode JMX error.\n {}'.format(e))

                return

        if not self.conf_nn.get('items') or not data.get('beans'):
            return

        for item in data['beans']:
            for c_item in self.conf_nn.get('items', []):
                if c_item['name'] == item['name']:
                    self._parse_with_filter(item, ['namenode'], metrics, c_item)

    def _collect_resourcemanager_metrics(self, metrics, dummy):
        if dummy:
            with open(DUMMY_INPUT_FILE_RM, "r") as read_metric_file:
                data = json.load(read_metric_file)
        else:
            try:
                data = self._call_json_api(self.conf_rm['url'], headers={'Accept': 'application/json'})
            except Exception as e:
                logging.error('Call Resource Manager metrics error.\n {}'.format(e))

                return

        if not data.get('clusterMetrics'):
            return

        self._parse_with_filter(data['clusterMetrics'], ['resourcemanager'], metrics, self.conf_rm)

    def _collect(self, dummy=False):
        metrics = {}
        self._collect_namenode_metrics(metrics, dummy)
        self._collect_resourcemanager_metrics(metrics, dummy)
        self._collect_ambari_alerts(metrics, dummy)

        # print json.dumps(metrics, indent=4)

        for k, v in metrics.items():
            # prom_metric = self.prom_metrics.get(k, None)

            # if not prom_metric:
            #     prom_metric = GaugeMetricFamily(k, k, labels=['cluster_name', 'component_name', 'host_name'])
            #     self.prom_metrics[k] = prom_metric

            # prom_metric.add_metric(list(labels.values()), v)

            self.statsd_metrics[k] = v

    def collect(self):
        if os.getenv('DUMMY_INPUT', 'FALSE') == 'TRUE':
            # Get dummy json data
            self._collect(dummy=True)
        else:
            logging.info('Start fetching metrics')
            self._collect()
            logging.info('Finish fetching metrics')

        # for m in list(self.prom_metrics.values()):
        #     yield m

        if os.getenv('STD_OUTPUT', 'FALSE') == 'TRUE':
            for k, v in self.statsd_metrics.items():
                print k, '=>', v
        else:
            logging.info('Send metrics to STATSD')
            for k, v in self.statsd_metrics.items():
                try:
                    statsd.gauge(k, v)
                except Exception as e:
                    logging.error('Send metrics to STATSD error.\n {}'.format(e))

    def _call_ambari_api(self, url):
        """
        Call Ambari's API to return json data
        Sample curl
        curl -k \
        -u username:password \
        -H 'X-Requested-By: ambari' \
        -X GET \
        "https://localhost:8080/api/v1/clusters/trustingsocial/host_components?fields=metrics/*"
        """

        ambari_info = self.ambari_info

        response = requests.get(
            url
            , auth=(ambari_info['AMBARI_USER'], ambari_info['AMBARI_PASS'])
            , headers={'X-Requested-By': 'ambari'}
            , verify=False
        )

        if response.status_code != requests.codes.ok:
            return {}
        return response.json()

    def _call_json_api(self, url, headers={}):
        response = requests.get(url, headers=headers)

        if response.status_code != requests.codes.ok:
            return {}
        return response.json()


if __name__ == "__main__":
    collector = AmbariMetricCollector(CONF_FILE)
    # Just for debug
    # for i in collector.collect():
    #     print(i)

    # REGISTRY.register(collector)
    # start_http_server(9999)
    # while True:
    #     time.sleep(1)

    if os.getenv('DEV', 'FALSE') == 'TRUE':
        # collector.debug_conf()
        collector.collect()
    else:
        while True:
            collector.collect()
            logging.info('Sleep {} s'.format(str(BATCH_DELAY)))
            time.sleep(int(BATCH_DELAY))
