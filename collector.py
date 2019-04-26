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

DUMMY_INPUT_FILE = os.getenv('DUMMY_INPUT_FILE', 'dummy_data/ambari-metrics-host-component.json')
BATCH_DELAY = os.getenv('BATCH_DELAY', 60)
BLACK_LIST_FILE = 'conf/black_list.yaml'


class AmbariMetricCollector(object):
    def __init__(self, black_list_file):
        # self.prom_metrics = {}
        self.statsd_metrics = {}
        self.ambari_info = {
            'AMBARI_URI': os.getenv('AMBARI_URI')
            , 'AMBARI_USER': os.getenv('AMBARI_USER')
            , 'AMBARI_PASS': os.getenv('AMBARI_PASS')
        }

        with open(black_list_file, "r") as read_bl_file:
            bl_conf = yaml.load(read_bl_file)

        self.metrics_bl, self.labels_bl = self._parse_black_list(bl_conf)

    def _parse_metrics(self, metrics, prefix, acc):
        for k, v in metrics.items():
            if type(v) is dict:
                prefix.append(k)
                self._parse_metrics(v, prefix, acc)
                if len(prefix) > 0:
                    prefix.pop(len(prefix) - 1)
            else:
                # metric_name = '{}_{}'.format('_'.join(prefix), k)
                metric_name = '{}.{}'.format('.'.join(prefix), k)

                if type(v) is int or type(v) is float:
                    acc[metric_name] = v

    def _is_label_black_list(self, labels, labels_bl):
        for k, vs in labels_bl.items():
            label_value = labels.get(k, None)
            if label_value:
                for v in vs:
                    if re.search(v, label_value):
                        return True
        return False

    def _filter_metric_in_black_list(self, metrics, metrics_bl):
        remove_metrics = []
        for bl in metrics_bl:
            for k, v in metrics.items():
                if re.search(bl, k):
                    remove_metrics.append(k)

        for rm in remove_metrics:
            del metrics[rm]

        return metrics

    def _parse(self, item, labels_bl, metrics_bl):
        labels = {'cluster_name': item['HostRoles']['cluster_name'],
                  'component_name': item['HostRoles']['component_name'],
                  'host_name': item['HostRoles']['host_name']}

        metrics = {}

        if not self._is_label_black_list(labels, labels_bl):
            self._parse_metrics(item['metrics'], [], metrics)

        metrics = self._filter_metric_in_black_list(metrics, metrics_bl)

        return labels, metrics

    def _parse_black_list(self, conf):
        black_list = conf.get('blacklist', None)

        if black_list:
            labels_black_list = black_list.get('labels', None)
            metrics_black_list = black_list.get('metric_names', None)

            return metrics_black_list, labels_black_list

        return None, None

    def _collect(self, metric_file=None):
        if metric_file:
            with open(metric_file, "r") as read_metric_file:
                data = json.load(read_metric_file)
        else:
            try:
                data = self._get_ambari_metrics()
            except Exception as e:
                logging.error('Call Ambari API error.\n {}'.format(e))

                return

        for item in data['items']:
            if item.get('metrics', None):
                labels, metrics = self._parse(item, self.labels_bl, self.metrics_bl)

                if metrics:
                    for k, v in metrics.items():
                        # prom_metric = self.prom_metrics.get(k, None)

                        # if not prom_metric:
                        #     prom_metric = GaugeMetricFamily(k, k, labels=['cluster_name', 'component_name', 'host_name'])
                        #     self.prom_metrics[k] = prom_metric

                        # prom_metric.add_metric(list(labels.values()), v)

                        statd_key = '{cluster_name}.{component_name}.{host_name}.{m_key}'.format(
                                cluster_name=labels['cluster_name']
                                , component_name=labels['component_name']
                                , host_name=labels['host_name'].replace('.', '-')
                                , m_key=k
                            )
                        self.statsd_metrics[statd_key] = v

    def collect(self):
        if os.getenv('DUMMY_INPUT', 'FALSE') == 'TRUE':
            # Get dummy json data
            self._collect(DUMMY_INPUT_FILE)
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

    def _get_ambari_metrics(self):
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

        return requests.get(
            '{}/api/v1/clusters/trustingsocial/host_components?fields=metrics/*'.format(ambari_info['AMBARI_URI'])
            , auth=(ambari_info['AMBARI_USER'], ambari_info['AMBARI_PASS'])
            , headers={'X-Requested-By': 'ambari'}
            , verify=False
        ).json()


if __name__ == "__main__":
    collector = AmbariMetricCollector(BLACK_LIST_FILE)
    # Just for debug
    # for i in collector.collect():
    #     print(i)

    # REGISTRY.register(collector)
    # start_http_server(9999)
    # while True:
    #     time.sleep(1)

    if os.getenv('DEV', 'FALSE') == 'TRUE':
        collector.collect()
    else:
        while True:
            collector.collect()
            logging.info('Sleep {} s'.format(str(BATCH_DELAY)))
            time.sleep(int(BATCH_DELAY))
