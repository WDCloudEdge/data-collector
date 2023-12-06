from Config import Config
import pickle
from util.KubernetesClient import KubernetesClient
import requests
import os
from handler.TraceNoIstio import handle_traces_no_istio
from handler.TraceIstio import handle_traces_istio
from handler.Trace import get_latency


def collect_from_svc(config: Config, _dir: str):
    global global_namespace_svcs_dict
    global_namespace_svcs_dict = KubernetesClient(config).get_all_svc()

    # 确定命名空间中需要访问的服务
    svcs = global_namespace_svcs_dict[config.namespace]
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
    net_latency = {}

    for svc in svcs:
        if config.namespace == 'cloud-sock-shop' or config.namespace == 'horsecoder-test':
            url = '{}end={}&start={}&limit={}&lookback={}&maxDuration&minDuration&service={}' \
                .format(config.jaeger_url, config.end * 1000000, config.start * 1000000, config.limit, config.lookBack,
                        svc)
            normal_dict, inbound_dict, outbound_dict, abnormal_dict, inbound_half_dict, outbound_half_dict, abnormal_half_dict = handle_traces_no_istio(
                pull(url), config)
        else:
            url = '{}end={}&start={}&limit={}&lookback={}&maxDuration&minDuration&service={}' \
                .format(config.jaeger_url, config.end * 1000000, config.start * 1000000, config.limit, config.lookBack,
                        svc + '.' + config.namespace)
            normal_dict, inbound_dict, outbound_dict, abnormal_dict, inbound_half_dict, outbound_half_dict, abnormal_half_dict = handle_traces_istio(
                pull(url), config)
        normal_dicts = {**normal_dicts, **normal_dict}
        inbound_dicts = {**inbound_dicts, **inbound_dict}
        outbound_dicts = {**outbound_dicts, **outbound_dict}
        abnormal_dicts = {**abnormal_dicts, **abnormal_dict}
        inbound_half_dicts = {**inbound_half_dicts, **inbound_half_dict}
        outbound_half_dicts = {**outbound_half_dicts, **outbound_half_dict}
        abnormal_half_dicts = {**abnormal_half_dicts, **abnormal_half_dict}

    # 处理pod_latency和net_latency数据
    pod_latency, net_latency = get_latency([normal_dicts, abnormal_dicts])

    normal_path = _dir + '/' + 'normal.pkl'
    inbound_path = _dir + '/' + 'inbound.pkl'
    outbound_path = _dir + '/' + 'outbound.pkl'
    abnormal_path = _dir + '/' + 'abnormal.pkl'
    inbound_half_path = _dir + '/' + 'inbound_half.pkl'
    outbound_half_path = _dir + '/' + 'outbound_half.pkl'
    abnormal_half_path = _dir + '/' + 'abnormal_half.pkl'
    trace_pod_latency_path = _dir + '/' + 'trace_pod_latency.pkl'
    trace_net_latency_path = _dir + '/' + 'trace_net_latency.pkl'

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
    with open(trace_net_latency_path, 'ab') as fw:
        pickle.dump(net_latency, fw)


def pull(url):
    '''
        拉取trace
    '''
    response = requests.get(url)
    return response.json()['data']


def collect_from_url(config: Config, url):
    # 自定义url
    if config.namespace == 'cloud-sock-shop' or config.namespace == 'horsecoder-test':
        normal_dict, inbound_dict, outbound_dict, abnormal_dict, inbound_half_dict, outbound_half_dict, abnormal_half_dict, pod_latency = handle_traces_no_istio(
            pull(url), config)
    else:
        normal_dict, inbound_dict, outbound_dict, abnormal_dict, inbound_half_dict, outbound_half_dict, abnormal_half_dict, pod_latency = handle_traces_istio(
            pull(url), config)
    # 自定义输出
    print("normal: ", normal_dict)
    print("inbound: ", inbound_dict)
    print("inbound_half: ", inbound_half_dict)
    print("outbound: ", outbound_dict)
    print("outbound_half: ", outbound_half_dict)
    print("abnormal: ", abnormal_dict)
    print("abnormal_half: ", abnormal_half_dict)



if __name__ == "__main__":
    # 作为服务收集数据
    namespaces = ['bookinfo', 'cloud-sock-shop', 'horsecoder-test']
    user_name = 'istio-test'
    global_now_time = 1701666000
    global_end_time = 1701667200
    # 作为单独测试的url参数，如果选择根据服务收集trace则无需修改
    url = 'http://47.99.200.176:16686/api/traces/0660ecc6bffa5c628fa7b166468b03a7'
    namespace = 'bookinfo'
    # 标志位
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
        config.namespace = namespace
        collect_from_url(config, url)
