from Config import Config
import pickle
from util.KubernetesClient import KubernetesClient
import requests
import os
from handler.TraceNoIstio import handle_traces_no_istio


def collect_from_svc(config: Config, _dir: str):
    global global_namespace_svcs_dict
    global_namespace_svcs_dict = KubernetesClient(config).get_all_svc()

    # 确定命名空间中需要访问的服务
    svcs = [svc for svc in config.svcs if
            'unknown' not in svc and 'redis' not in svc and 'istio-ingressgateway' not in svc]
    if config.namespace == 'horsecoder-test':
        svcs = ['edge-llm-1.0.0', 'edge-paraformer-serverless-1.0.0', 'edge-gateway']

    normal_dicts = {}
    inbound_dicts = {}
    outbound_dicts = {}
    abnormal_dicts = {}
    inbound_half_dicts = {}
    outbound_half_dicts = {}
    abnormal_half_dicts = {}
    pod_latency = {}

    for svc in svcs:
        url = '{}end={}&start={}&limit={}&lookback={}&maxDuration&minDuration&service={}' \
            .format(config.jaeger_url, config.end * 1000000, config.start * 1000000, config.limit, config.lookBack,
                    svc)
        normal_dict, inbound_dict, outbound_dict, abnormal_dict, inbound_half_dict, outbound_half_dict, abnormal_half_dict, pod_latency = handle_traces_no_istio(
            pull(url), config)
        normal_dicts = {**normal_dicts, **normal_dict}
        inbound_dicts = {**inbound_dicts, **inbound_dict}
        outbound_dicts = {**outbound_dicts, **outbound_dict}
        abnormal_dicts = {**abnormal_dicts, **abnormal_dict}
        inbound_half_dicts = {**inbound_half_dicts, **inbound_half_dict}
        outbound_half_dicts = {**outbound_half_dicts, **outbound_half_dict}
        abnormal_half_dicts = {**abnormal_half_dicts, **abnormal_half_dict}

    normal_path = _dir + '/' + 'normal.pkl'
    inbound_path = _dir + '/' + 'inbound.pkl'
    outbound_path = _dir + '/' + 'outbound.pkl'
    abnormal_path = _dir + '/' + 'abnormal.pkl'
    inbound_half_path = _dir + '/' + 'inbound_half.pkl'
    outbound_half_path = _dir + '/' + 'outbound_half.pkl'
    abnormal_half_path = _dir + '/' + 'abnormal_half.pkl'
    trace_pod_latency_path = _dir + '/' + 'trace_pod_latency.pkl'

    with open(normal_path, 'ab') as fw:
        pickle.dump(list(normal_dicts.values()), fw)
    with open(inbound_path, 'ab') as fw:
        pickle.dump(list(inbound_dicts.values()), fw)
    with open(outbound_path, 'ab') as fw:
        pickle.dump(list(outbound_dicts.values()), fw)
    with open(abnormal_path, 'ab') as fw:
        pickle.dump(list(abnormal_dicts.values()), fw)
    with open(inbound_half_path, 'ab') as fw:
        pickle.dump(list(inbound_half_dicts.values()), fw)
    with open(outbound_half_path, 'ab') as fw:
        pickle.dump(list(outbound_half_dicts.values()), fw)
    with open(abnormal_half_path, 'ab') as fw:
        pickle.dump(list(abnormal_half_dicts.values()), fw)
    with open(trace_pod_latency_path, 'ab') as fw:
        pickle.dump(pod_latency, fw)


def pull(url):
    '''
        拉取trace
    '''
    response = requests.get(url)
    return response.json()['data']


def collect_from_url(config: Config):
    # 自定义url
    url = '{}end={}&start={}&limit={}&lookback={}&maxDuration&minDuration&service={}' \
        .format(config.jaeger_url, config.end * 1000000, config.start * 1000000, config.limit, config.lookBack,
                'carts')
    normal_dict, inbound_dict, outbound_dict, abnormal_dict, inbound_half_dict, outbound_half_dict, abnormal_half_dict, pod_latency = handle_traces_no_istio(
        pull(url), config)
    # 自定义输出
    for key in normal_dict.keys():
        print(normal_dict[key])



if __name__ == "__main__":
    namespaces = ['cloud-sock-shop', 'horsecoder-test']
    user_name = 'no-istio-test'
    global_now_time = 1701666000
    global_end_time = 1701667200
    is_svc = True

    config = Config()
    config.user = user_name
    folder = '.'

    if is_svc is True:
    # 根据服务收集
        for n in namespaces:
            config.namespace = n
            config.svcs.clear()
            config.pods.clear()
            config.start = global_now_time
            config.end = global_end_time
            data_folder = './data/' + str(config.user) + '/' + config.namespace
            if not os.path.exists(data_folder):
                os.makedirs(data_folder)
            collect_from_svc(config, data_folder)
    else:
        # 根据url收集
        collect_from_url(config)