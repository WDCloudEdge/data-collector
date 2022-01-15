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
        self.jaeger_url = 'http://121.4.170.179:15033/api/traces?'
        self.lookBack = 'custom'
        self.limit = 1000000

        # kiali (log)
        self.kiali_url = 'http://121.4.170.179:15029/kiali/api'
        self.kiali_cookie = 'kiali-token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2NDIzMjMyNzksImlzcyI6ImtpYWxpLWxvZ2luIiwic3ViIjoiYWRtaW4ifQ.QiSiSeMIGba2XmmBB2PjIGeDp-27Ucidy9RivT1G1_M; _ga=GA1.1.1033189754.1637066853; _xsrf=2|54022c23|8d9344a7bc8fe2db138db5f93a0d2c9e|1639745173; _gid=GA1.1.1582671738.1642151252'
