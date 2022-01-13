import requests
from Config import Config
import pickle


def collect(config: Config):
    pod_url = build_log_urls(config)
    for pod in pod_url.keys():
        res = execute(pod_url[pod])
        pipe_path='data/log/%s.pkl' % pod
        with open(pipe_path,'wb') as fw:
            pickle.dump(res,fw)


def build_log_urls(config: Config):
    '''
        为每一个服务构建获取log的地址
        注意，如果log数量过多，可以用只取后xx行来限制 &tailLines=xx 来限制
    '''
    pod_url = {}
    for pod in config.pods:
        url = config.kiali_url + '/namespaces/{}/pods/{}/logs?container=server&sinceTime={}'.format(config.namespace, pod, int(config.start/1e6))
        pod_url[pod] = url
    return pod_url

def execute(url):
    '''
        拉取log
    '''
    headers = {
        "Cookie": 'kiali-token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE2NDIwNjI1NDYsImlzcyI6ImtpYWxpLWxvZ2luIiwic3ViIjoiYWRtaW4ifQ.8u5HexwbAvtWWuGcElZ2KzEaYz7qoh2m1cHFq_Hflpw; _ga=GA1.1.1033189754.1637066853; _xsrf=2|54022c23|8d9344a7bc8fe2db138db5f93a0d2c9e|1639745173; _gid=GA1.1.1711183228.1641955364'
    }
    response = requests.get(url, headers=headers)
    return response.json()