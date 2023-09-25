import time


class Config:
    def __init__(self):
        self.namespace = 'bookinfo'
        self.nodes = {
            'master': 'izbp193ioajdcnpofhlr1hz',
            'cloud-worker1': 'izbp1gwb52uyj3g0wn52lez',
            'cloud-worker2': 'izbp1gwb52uyj3g0wn52lfz',
            # 'edge-worker1': 'dell2015',
            'edge-worker2': 'dell2018',
        }
        self.svcs = set()
        self.pods = set()

        # duration
        self.duration = 10 * 60
        self.start = int(round((time.time() - self.duration)))
        self.end = int(round(time.time())) - 60 * 60 * 21

        # prometheus
        self.prom_range_url = "http://47.99.240.112:31444/api/v1/query_range"
        self.prom_range_url_node = "http://47.99.240.112:31222/api/v1/query_range"
        self.prom_no_range_url = "http://47.99.240.112:31444/api/v1/query"
        self.step = 5

        # jarger
        self.jaeger_url = 'http://47.99.200.176:14268/jaeger/api/traces?'
        self.lookBack = 'custom'
        self.limit = 1000

        # kubernetes
        self.k8s_config = 'local-config'

        # concurrency set
        self.user = 30
