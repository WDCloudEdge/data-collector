import time

class Config():
    def __init__(self):

        self.namespace = 'hipster'
        self.nodes = {
            'ubuntu-Precision-Tower-7810': '121.4.170.179:9100'
        }
        self.svcs = set()
        self.pods = set()
        
        # duration
        self.duration = 10 * 60
        self.start = int(round((time.time() - self.duration) * (10 ** 6)))
        self.end = int(round(time.time() * (10 ** 6)))

        # prometheus
        self.prom_range_url = "http://121.4.170.179:15030/api/v1/query_range"
        self.prom_no_range_url = "http://121.4.170.179:15030/api/v1/query"
        self.step = 1

        # jarger
        self.jaeger_url = 'http://121.4.170.179:15033/api/traces?'
        self.lookBack = 'custom'
        self.limit = 1000000


    def build_trace_urls(self):
        # 为每一个服务拼接一个获取地址
        urls = ['{}end={}&start={}&limit={}&lookback={}&maxDuration&minDuration&service={}' \
                .format(self.jaeger_url, self.end, self.start, self.limit, self.lookBack, svc) for svc in self.svcs]           
        return urls