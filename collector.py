import json
import yaml
import re
import time
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY


class AmbariMetricCollector(object):
    def __init__(self):
        self.prom_metrics = {}

    def _parse_metrics(self, metrics, prefix, acc):
        for k, v in metrics.items():
            if type(v) is dict:
                prefix.append(k)
                self._parse_metrics(v, prefix, acc)
                if len(prefix) > 0:
                    prefix.pop(len(prefix) - 1)
            else:
                metric_name = '{}_{}'.format('_'.join(prefix), k)

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

    def _collect(self, metric_file, black_list_file):
        with open(metric_file, "r") as read_metric_file:
            data = json.load(read_metric_file)

        with open(black_list_file, "r") as read_bl_file:
            bl_conf = yaml.load(read_bl_file)

        metrics_bl, labels_bl = self._parse_black_list(bl_conf)
        for item in data['items']:
            if item.get('metrics', None):
                labels, metrics = self._parse(item, labels_bl, metrics_bl)

                if metrics:
                    for k, v in metrics.items():
                        prom_metric = self.prom_metrics.get(k, None)

                        if not prom_metric:
                            prom_metric = GaugeMetricFamily(k, k, labels=['cluster_name', 'component_name', 'host_name'])
                            self.prom_metrics[k] = prom_metric

                        prom_metric.add_metric(list(labels.values()), v)

                        yield prom_metric

    def collect(self):
        return self._collect('conf/ambari-metrics-host-component.json',
                             'conf/black_list.yaml')


if __name__ == "__main__":
    collector = AmbariMetricCollector()
    # Just for debug
    # for i in collector.collect():
    #     print(i)

    REGISTRY.register(collector)
    start_http_server(9999)
    while True:
        time.sleep(1)
