import time


class Config:
    def __init__(self):
        self.namespace = 'bookinfo'
        self.nodes = {
            'master': 'izbp193ioajdcnpofhlr1hz',
            'cloud-worker1': 'izbp1gwb52uyj3g0wn52lez',
            'cloud-worker2': 'izbp1gwb52uyj3g0wn52lfz',
            'cloud-worker3': 'izbp16opgy3xucvexwqp9dz',
            'edge-worker1': 'server-1',
            'edge-worker2': 'server-2',
            'edge-worker3': 'server-3',
            'edge-worker4': 'dell2018',
        }
        self.svcs = set()
        self.pods = set()

        self.interval = 1
        # duration related to interval
        self.duration = self.interval
        self.start = int(round((time.time() - self.duration)))
        self.end = int(round(time.time()))

        # prometheus
        self.prom_range_url = "http://47.99.240.112:31444/api/v1/query_range"
        self.prom_range_url_node = "http://47.99.240.112:31222/api/v1/query_range"
        self.prom_no_range_url = "http://47.99.240.112:31444/api/v1/query"
        # step related to interval
        self.step = self.interval

        # jarger
        self.jaeger_url = 'http://47.99.200.176:16686/api/traces?'
        self.lookBack = str(int(self.duration / 60)) + 'm'
        self.limit = 1000

        # kubernetes
        self.k8s_config = 'local-config'

        # concurrency set
        self.user = '100-hybrid'
