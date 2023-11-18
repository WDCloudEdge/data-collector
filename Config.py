import time


class Config:
    def __init__(self):
        self.namespace = 'bookinfo'
        self.nodes = [
            Node('master', '172.26.146.178', 'izbp193ioajdcnpofhlr1hz'),
            Node('cloud-worker1', '172.26.146.180', 'izbp1gwb52uyj3g0wn52lez'),
            Node('cloud-worker2', '172.26.146.179', 'izbp1gwb52uyj3g0wn52lfz'),
            Node('cloud-worker3', '172.23.182.14', 'izbp16opgy3xucvexwqp9dz'),
            Node('edge-worker1', '192.168.31.74', 'server-1'),
            Node('edge-worker2', '192.168.31.85', 'server-2'),
            Node('edge-worker3', '192.168.31.128', 'server-3'),
            Node('edge-worker4', '192.168.31.208', 'dell2018')
        ]
        self.svcs = set()
        self.pods = set()

        self.interval = 10 * 60
        # duration related to interval
        self.duration = self.interval
        self.start = int(round((time.time() - self.duration)))
        self.end = int(round(time.time()))

        # prometheus
        self.prom_range_url = "http://47.99.240.112:31444/api/v1/query_range"
        self.prom_range_url_node = "http://47.99.240.112:31222/api/v1/query_range"
        self.prom_no_range_url = "http://47.99.240.112:31444/api/v1/query"
        self.step = 5

        # jarger
        self.jaeger_url = 'http://47.99.200.176:16686/api/traces?'
        self.lookBack = str(int(self.duration / 60)) + 'm'
        self.limit = 100000

        # kubernetes
        self.k8s_config = 'local-config'

        # concurrency set
        self.user = '20user-hybrid-20231118'


class Node:
    def __init__(self, name, ip, node_name):
        self.name = name
        self.ip = ip
        self.node_name = node_name
