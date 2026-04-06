import time

class Config:
    def __init__(self):
        self.namespace = 'bookinfo'
        self.nodes = None
        self.svcs = set()
        self.pods = set()

        self.interval = 5 * 60  # 每次收集数据的时间（5min）
        # duration related to interval
        self.duration = self.interval
        # self.start = int(round((time.time() - self.duration)))
        # 10
        # self.end = 1774320780
        # 30
        # self.end = 1774322160
        # 50
        # self.end = 1774322880
        # 70
        # self.end = 1774325520
        # 90
        # self.end = 1774326420

        # 10 hipster zheng shi
        # self.end = 1774343100
        # 30
        # self.end = 1774343700
        # 50
        # self.end = 1774344180
        # 70
        # self.end = 1774344720
        # 90
        # self.end = 1774345320

        # 10 bookinfo zheng shi
        # self.end = 1774348560
        # 30
        # self.end = 1774349460
        # 50
        # self.end = 1774350000
        # 70
        # self.end = 1774350480
        # 90
        self.end = 1775405520

        # python3 -c "import time; print(int(time.mktime(time.strptime('2026-03-24 22:55:00','%Y-%m-%d %H:%M:%S'))))"
        self.start = self.end - self.duration

        # prometheus
        # self.prom_range_url = "http://192.168.31.227:30210/api/v1/query_range"  # istio支持
        self.prom_range_url = "http://192.168.31.227:30202/api/v1/query_range"  # istio支持
        self.prom_range_url_node = "http://192.168.31.227:30200/api/v1/query_range"  # 原生Prometheus
        self.prom_no_range_url_node = "http://192.168.31.227:30200/api/v1/query"
        # self.prom_no_range_url = "http://192.168.31.227:30210/api/v1/query"
        self.prom_no_range_url = "http://192.168.31.227:30202/api/v1/query"
        self.step = 5

        # jaeger
        self.jaeger_url = 'http://192.168.31.171:16686/api/traces?'
        self.lookBack = str(int(self.duration / 60)) + 'm'
        self.limit = 100000

        # kiali
        self.kiali_url = 'http://47.99.200.176:32001/kiali/api'

        # kubernetes
        self.k8s_config = 'config.yaml'  # kubernetes配置文件地址

        # dir name
        self.user = '90user-5min'


class Node:
    def __init__(self, name, ip, node_name, cni_ip, status):
        self.name = name
        self.ip = ip
        self.node_name = node_name
        self.cni_ip = cni_ip
        self.status = status


class Pod:
    def __init__(self, node, namespace, host_ip, ip, name):
        self.node = node
        self.namespace = namespace
        self.host_ip = host_ip
        self.ip = ip
        self.name = name
