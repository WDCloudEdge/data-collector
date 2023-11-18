import time


class Config:
    def __init__(self):
        self.namespace = 'bookinfo'  # 命名空间（默认）
        self.nodes = {  # 节点名
            'master': 'izbp193ioajdcnpofhlr1hz',
            'cloud-worker1': 'izbp1gwb52uyj3g0wn52lez',
            'cloud-worker2': 'izbp1gwb52uyj3g0wn52lfz',
            'cloud-worker3': 'izbp16opgy3xucvexwqp9dz',
            'edge-worker1': 'server-1',
            'edge-worker2': 'server-2',
            'edge-worker3': 'server-3',
            'edge-worker4': 'dell2018',
        }
        self.svcs = set()  # 服务
        self.pods = set()  # 实例

        self.interval = 10 * 60  # 每次收集数据的时间（10min）
        # duration related to interval
        self.duration = self.interval
        self.start = int(round((time.time() - self.duration)))
        self.end = int(round(time.time()))

        # prometheus
        self.prom_range_url = "http://47.99.240.112:31444/api/v1/query_range"  # istio支持
        self.prom_range_url_node = "http://47.99.240.112:31222/api/v1/query_range"  # 原生Prometheus
        self.prom_no_range_url = "http://47.99.240.112:31444/api/v1/query"
        self.step = 5

        # jarger
        self.jaeger_url = 'http://47.99.200.176:16686/api/traces?'
        self.lookBack = str(int(self.duration / 60)) + 'm'
        self.limit = 100000

        # kubernetes
        self.k8s_config = 'config.yaml'  # kubernetes配置文件地址

        # concurrency set
        self.user = '10user-hybrid'  # 测试数据名
