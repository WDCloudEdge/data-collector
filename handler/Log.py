import requests
from Config import Config


def collect(config: Config):
    pod_url = build_log_urls(config)
    for pod in pod_url.keys():
        res = execute(pod_url[pod], config)
        pipe_path = 'data/log/%s.txt' % pod
        with open(pipe_path, 'w') as fw:
            if 'logs' in res.keys():
                fw.write(res['logs'])
            else:
                fw.write('')


def build_log_urls(config: Config):
    '''
        为每一个服务构建获取log的地址
        注意，如果log数量过多，可以用只取后xx行来限制 &tailLines=xx 来限制
    '''
    pod_url = {}
    for pod in config.pods:
        url = config.kiali_url + '/namespaces/{}/pods/{}/logs?container=server&sinceTime={}'.format(config.namespace,
                                                                                                    pod,
                                                                                                    int(config.start / 1e6))
        pod_url[pod] = url
    return pod_url


def execute(url, config: Config):
    '''
        拉取log
    '''
    headers = {
        "Cookie": config.kiali_cookie
    }
    response = requests.get(url, headers=headers)
    return response.json()
