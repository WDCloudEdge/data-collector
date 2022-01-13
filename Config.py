import time

class Config():
    def __init__(self):

        self.namespace = 'hipster'
        self.nodes = {
            'ubuntu-Precision-Tower-7810': '121.4.170.179:9100'
        }
        self.svcs = set()
        self.pods = set()
        
        # duration 默认前10分钟
        self.duration = 10 * 60
        self.start = int(round((time.time() - self.duration) * (10 ** 6)))
        self.end = int(round(time.time() * (10 ** 6)))

        # prometheus
        self.prom_range_url = "http://121.4.170.179:15030/api/v1/query_range"
        self.prom_no_range_url = "http://121.4.170.179:15030/api/v1/query"
        self.step = 1

        # jarger
        self.jaeger_url = 'http://121.4.170.179:15032/jaeger/api/traces?'
        self.lookBack = 'custom'
        self.limit = 1000

        # kiali (log)
        self.kiali_url = 'http://121.4.170.179:15029/kiali/api'
