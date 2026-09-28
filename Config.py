import time

class Config:
    def __init__(self):
        self.namespace = 'agent-network'
        self.nodes = None
        self.svcs = set()
        self.pods = set()

        self.interval = 20 * 60  # 每次收集数据的时间（5min）
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
        # 200
        # self.end = 1774360380
        # 400
        # self.end = 1787750520
        #20260904下午14.10-14.15 normal-thingo-1user-5min
        # 14.20 - 15
        # self.end = 1788502800

        #20260904下午14.30-14.35 normal-thingo-5user-5min
        # 14.45 - 25
        # self.end = 1788504300

        #20260904下午14.30-14.35 normal-thingo-5user-5min
        # 14.50 - 30
        # self.end = 1788504600

        #20260906下午16:15-16.20 normal-thingo-10user-5min
        # 16.50 - 45
        # self.end = 1788684600

        # 20260907-21.00-21.05 normal-thingo-1user-5min-multi-21.10
        # 21.10 - 15
        # self.end = 1788786600

        # 20260907-21.00-21.05 normal-thingo-1user-5min-multi-21.10
        # 23.00 - 25
        # self.end = 1788792900

        # 20260911-13.25-13.30 normal-thingo-10user-5min-multi-13.34
        # 13.34 - 14
        # self.end = 1789104840

        # 20260911-13.25-13.30 normal-thingo-10user-5min-multi-13.45
        # 13.45 - 30
        # self.end = 1789105500

        # 20260911-13.25-13.30 normal-thingo-10user-5min-multi-13.45
        # 13.50 - 35
        # self.end = 1789105800

        # 20260911-19.05-19.10-normal-thingo-10user-5min-multi-19.30
        # 19.30 - 35
        # self.end = 1789126200

        # 20260911-19.05-19.10-normal-thingo-10user-5min-multi-19.35
        # 19.35 - 40
        # self.end = 1789126500

        # 20260912-17.55-18.00-normal-thingo-10user-5min-multi-18.15
        # 18.15 - 30
        # self.end = 1789208100

        # 20260913-14.10-14.15-normal-thingo-3user-5min-multi-14.20
        # 14.20-20
        # self.end = 1789280400

        # 20260913-14.40-14.45-normal-thingo-3user-5min-14.50
        # 14.50-20
        self.end = 1789282200

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
        # self.end = 1774351020
        # 200
        # self.end = 1774358700
        # 400
        # self.end = 1774359180
        # 600
        # self.end = 1774359660
        self.start = self.end - self.duration

        # prometheus
        # self.prom_range_url = "http://192.168.31.227:30210/api/v1/query_range"  # istio支持
        self.prom_range_url = "http://192.168.31.227:30202/api/v1/query_range"  # istio支持
        self.prom_range_url_node = "http://192.168.31.227:30200/api/v1/query_range"  # 原生Prometheus
        self.prom_no_range_url_node = "http://192.168.31.227:30200/api/v1/query"
        # self.prom_no_range_url = "http://192.168.31.227:30210/api/v1/query"
        self.prom_no_range_url = "http://192.168.31.227:30200/api/v1/query"
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
        # self.user = 'thingo/normal/20260904-14.10-14.15-normal-thingo-1user-5min_14.20'
        # self.user = 'thingo/normal/20260907-21.00-21.05-normal-thingo-1user-5min-multi-21.10'
        # self.user = 'thingo/normal/20260907-22.45-22.50-normal-thingo-5user-5min-multi-23.00'
        # self.user = 'thingo/normal/20260911-13.25-13.30-normal-thingo-10user-5min-multi-13.34'
        # self.user = 'thingo/normal/20260911-13.25-13.30-normal-thingo-10user-5min-multi-13.45'
        # self.user = 'thingo/normal/20260911-13.25-13.30-normal-thingo-10user-5min-multi-13.50'
        # self.user = 'thingo/normal/20260911-19.05-19.10-normal-thingo-10user-5min-multi-19.35'
        # self.user = 'thingo/normal/20260912-17.55-18.00-normal-thingo-10user-5min-multi-18.15'
        # self.user = 'thingo/normal/20260913-14.10-14.15-normal-thingo-3user-5min-multi-14.20'
        self.user = 'thingo/normal/20260913-14.40-14.45-normal-thingo-3user-5min-14.50'
        # self.user = 'thingo/normal/20260904-14.30-14.35-normal-thingo-5user-5min_14.50'
        # self.user = 'thingo/normal/20260906-16.15-16.20-normal-thingo-10user-5min_16.50'
        # self.user = 'abnormal-thingo-10user-10min-summarizer'


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
