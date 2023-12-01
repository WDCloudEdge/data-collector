import requests
from Config import Config
import pickle
import os


def collect(config: Config, _dir: str):
    # 确定命名空间中需要访问的服务
    svcs = [svc for svc in config.svcs if
            'unknown' not in svc and 'redis' not in svc and 'istio-ingressgateway' not in svc]
    # urls = build_trace_urls(config)
    normal_dicts = {}
    inbound_dicts = {}
    outbound_dicts = {}
    abnormal_dicts = {}
    inbound_half_dicts = {}
    outbound_half_dicts = {}
    abnormal_half_dicts = {}
    pod_latency = {}

    for svc in svcs:
        url = build_trace_url(config, svc + '.' + config.namespace)
        # trace_dict = handle(pull(url))
        normal_dict, inbound_dict, outbound_dict, abnormal_dict, inbound_half_dict, outbound_half_dict, abnormal_half_dict, pod_latency = handle_traces(
            pull(url), svc)
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


def build_trace_url(config: Config, svc):
    url = '{}end={}&start={}&limit={}&lookback={}&maxDuration&minDuration&service={}' \
        .format(config.jaeger_url, config.end * 1000000, config.start * 1000000, config.limit, config.lookBack,
                svc)
    return url


def build_trace_urls(config: Config):
    '''
        构建每个服务每分钟的trace拉取路径（避免数据量太大）
    '''
    svcs = [svc + '.' + config.namespace for svc in config.svcs if
            'unknown' not in svc and 'redis' not in svc and 'istio-ingressgateway' not in svc]
    urls = ['{}end={}&start={}&limit={}&lookback={}&maxDuration&minDuration&service={}' \
                .format(config.jaeger_url, config.end * 1000000, config.start * 1000000, config.limit, config.lookBack,
                        svc) for
            svc in
            svcs]
    return urls
    # windowsSize = 10
    # start = config.yaml.start
    # end = config.yaml.start + windowsSize # 10s一个windows
    # windows = []
    # while end <= config.yaml.end:
    #     windows.append((start, end))
    #     start += windowsSize
    #     end += windowsSize
    # # 为每一个滑动窗口的服务拼接一个获取地址
    # for window in windows:
    #     urls = ['{}end={}&start={}&limit={}&lookback={}&maxDuration&minDuration&service={}' \
    #         .format(config.yaml.jaeger_url, window[1], window[0], config.yaml.limit, config.yaml.lookBack, svc) for svc in svcs]
    # return urls


def pull(url):
    '''
        拉取trace
    '''
    response = requests.get(url)
    return response.json()['data']


def handle_istio_node_id(node_id: str) -> str:
    s = node_id.split('~')
    if node_id == 'OTHER_NODE':
        return 'OTHER_NODE'
    if len(s) != 4:
        raise Exception('istio trace node id illegal')
    return s[2] + '-' + s[1]


def handle_istio_pod_id(node_id: str) -> str:
    s = node_id.split('~')
    if len(s) != 4:
        raise Exception('istio trace node id illegal')
    return s[2].split('.')[0]


def handle_traces(trace_jsons, current_svc):
    # 需要构建的trace列表
    normal_dicts = {}
    inbound_dicts = {}
    outbound_dicts = {}
    abnormal_dicts = {}
    inbound_half_dicts = {}
    outbound_half_dicts = {}
    abnormal_half_dicts = {}
    pod_latency = {}
    # 遍历所有trace
    for trace_json in trace_jsons:

        # 定义标志位
        is_abnormal = False
        is_inbound = False
        is_outbound = False
        is_half = False  # 是否需要将其拆开

        # 定义一个trace
        half_trace_dict = {}
        trace_dict = {'traceId': trace_json['traceID']}
        # 定义trace字段
        trace_dict['call'] = []
        trace_dict['timestamp'] = []
        trace_dict['latency'] = []
        trace_dict['http_status'] = []
        trace_dict['svc'] = []
        trace_dict['call_instance'] = []

        # 建立标志位数组，存放没有被访问过的span
        span_flag = []

        # 获取trace中的所有span
        spans_dict = {}
        for span_json in trace_json['spans']:
            spans_dict[span_json['spanID']] = span_json
            span_flag.append(span_json['spanID'])

        # 遍历trace中的所有span，处理数据(链路中的所有span都是在本trace内寻找其他span的)
        for span_json in trace_json['spans']:
            # 获取基本信息
            trace_dict['timestamp'].append(span_json['startTime'])
            trace_dict['latency'].append(span_json['duration'])
            # 根据状态码判断是否正常
            for tag in span_json['tags']:
                if tag['key'] == 'http.status_code':
                    http_status = int(tag['value'])
                    trace_dict['http_status'].append(tag['value'])
                    if http_status < 200 or http_status >= 300:
                        is_abnormal = True

            # 设置变量
            node_id = None  # 节点id
            caller_svc = None  # 调用者服务名
            callee_svc = None  # 被调用者服务名
            outbound_node_id = None  # 出边的节点id
            namespace = None  # 命名空间
            outbound_namespace = None  # 出边的命名空间

            # 遍历span中的所有tag
            for tag in span_json['tags']:
                # 找到节点id
                if 'node_id' == tag['key']:
                    node_id = tag['value']
                # 找到被调用者的服务名（如果你是的话）
                if 'istio.canonical_service' == tag['key']:
                    callee_svc = tag['value']
                if 'istio.namespace' == tag['key']:
                    namespace = tag['value']

            # 查看是出边还是入边
            for tag in span_json['tags']:
                # 如果是入边，判断是否有引用
                if 'upstream_cluster' == tag['key']:
                    if 'inbound' in tag['value']:
                        # 移除该span的标志位
                        span_flag.remove(span_json['spanID'])
                        # 有引用，则找到相应的出边，绑定成一对
                        if span_json['references'] != []:
                            for ref in span_json['references']:
                                if ref['refType'] == 'CHILD_OF':
                                    # 找到出边的span
                                    outbound_span = spans_dict[ref['spanID']]
                                    # 移除该span的标志位
                                    if ref['spanID'] in span_flag:
                                        span_flag.remove(ref['spanID'])
                                    # 获取出边的信息
                                    for outbound_tag in outbound_span['tags']:
                                        # 找到出边的节点id
                                        if 'node_id' == outbound_tag['key']:
                                            outbound_node_id = outbound_tag['value']
                                        # 找到调用者的服务名
                                        if 'istio.canonical_service' == outbound_tag['key']:
                                            caller_svc = outbound_tag['value']
                                            # 如果服务名是unknown，则将其替换
                                            if caller_svc == 'unknown':
                                                caller_svc = 'istio-ingressgateway'
                                        if 'istio.namespace' == outbound_tag['key']:
                                            outbound_namespace = outbound_tag['value']
                                    # 判断是否跨系统（通过命名空间取判断）
                                    # 判断是出边还是入边(通过服务名去判断)
                                    # 改变标志位
                                    if caller_svc == 'istio-ingressgateway':
                                        outbound_namespace = namespace
                                    if namespace != outbound_namespace:
                                        # 是跨系统的trace，判断是入边还是出边，并构建截断的trace_dict
                                        half_trace_dict = get_half_trace(trace_json)
                                        is_half = True
                                        if current_svc != callee_svc:
                                            # 证明目前该方是调用者，为outbound
                                            is_outbound = True
                                            half_trace_dict['timestamp'][len(half_trace_dict['timestamp']) - 1] = -1
                                            half_trace_dict['latency'][len(half_trace_dict['latency']) - 1] = -1
                                            half_trace_dict['http_status'][len(half_trace_dict['http_status']) - 1] = -1
                                            half_trace_dict['svc'].append(caller_svc)
                                            half_trace_dict['call'].append((caller_svc, 'OTHER_SYSTEM'))
                                            half_trace_dict['call_instance'].append(
                                                (handle_istio_node_id(node_id), handle_istio_node_id('OTHER_NODE')))
                                        else:
                                            is_inbound = True
                                            half_trace_dict['timestamp'][len(half_trace_dict['timestamp']) - 2] = -1
                                            half_trace_dict['latency'][len(half_trace_dict['latency']) - 2] = -1
                                            half_trace_dict['http_status'][len(half_trace_dict['http_status']) - 2] = -1
                                            half_trace_dict['svc'].append(callee_svc)
                                            half_trace_dict['call'].append(('OTHER_SYSTEM', callee_svc))
                                            half_trace_dict['call_instance'].append(
                                                (handle_istio_node_id('OTHER_NODE'), handle_istio_node_id(node_id)))

                        # 如果没有引用，则用特定字符替代
                        else:
                            caller_svc = 'OTHER_SYSTEM'
                            outbound_node_id = 'OTHER_NODE'
                            # 为入边，改变标志位
                            is_inbound = True
                            # 其他信息
                            trace_dict['timestamp'].insert(len(trace_dict['timestamp']) - 1, -1)
                            trace_dict['latency'].insert(len(trace_dict['latency']) - 1, -1)
                            trace_dict['http_status'].insert(len(trace_dict['http_status']) - 1, -1)
                        # 将调用关系添加到trace中
                        trace_dict['svc'].append(callee_svc)
                        # 补全信息
                        if node_id is not None:
                            trace_dict['call'].append((caller_svc, callee_svc))
                            trace_dict['call_instance'].append(
                                (handle_istio_node_id(outbound_node_id), handle_istio_node_id(node_id)))
                            if outbound_node_id != 'OTHER_NODE' and node_id != 'OTHER_NODE':
                                pod_latency_key = handle_istio_pod_id(outbound_node_id) + '&' + handle_istio_pod_id(
                                    node_id)
                                pod_latency_list = pod_latency.get(pod_latency_key, [])
                                pod_latency_list.append(span_json['duration'])
                                pod_latency[pod_latency_key] = pod_latency_list

        # 再次进行遍历，找到没有对应调用关系的出边
        for span_id in span_flag:
            span = spans_dict[span_id]
            node_id = None
            caller_svc = None
            # 获取信息
            for span_tag in span['tags']:
                # 找到节点id
                if 'node_id' == span_tag['key']:
                    node_id = span_tag['value']
                # 找到服务名
                if 'istio.canonical_service' == span_tag['key']:
                    caller_svc = span_tag['value']

            # 判断是否为出边，有则将其加入到trace中
            for span_tag in span['tags']:
                if 'upstream_cluster' == span_tag['key']:
                    if 'outbound' in span_tag['value']:
                        # 将其移除标志位
                        span_flag.remove(span['spanID'])
                        trace_dict['svc'].append('OTHER_SERVICE')
                        # 补全信息
                        if node_id is not None:
                            trace_dict['call'].append((caller_svc, 'OTHER_SYSTEM'))
                            trace_dict['call_instance'].append(
                                (handle_istio_node_id(node_id), handle_istio_node_id('OTHER_NODE')))
                            # 其他信息
                            trace_dict['timestamp'].insert(len(trace_dict['timestamp']), -1)
                            trace_dict['latency'].insert(len(trace_dict['latency']), -1)
                            trace_dict['http_status'].insert(len(trace_dict['latency']), -1)
                        # 为出边，改变标志位
                        is_outbound = True

        # 再次进行遍历，判断是否为异常状况
        if span_flag != []:
            is_abnormal = True

        # 根据标志位判断是否加入各trace信息中
        if is_abnormal == True:
            abnormal_dicts[trace_json['traceID']] = trace_dict
            if is_half == True:
                abnormal_half_dicts[trace_json['traceID']] = half_trace_dict
            else:
                abnormal_half_dicts[trace_json['traceID']] = trace_dict
        else:
            normal_dicts[trace_json['traceID']] = trace_dict
        if is_inbound == True:
            inbound_dicts[trace_json['traceID']] = trace_dict
            if is_half == True:
                inbound_half_dicts[trace_json['traceID']] = half_trace_dict
            else:
                inbound_half_dicts[trace_json['traceID']] = trace_dict
        if is_outbound == True:
            outbound_dicts[trace_json['traceID']] = trace_dict
            if is_half == True:
                outbound_half_dicts[trace_json['traceID']] = half_trace_dict
            else:
                outbound_half_dicts[trace_json['traceID']] = trace_dict
    return normal_dicts, inbound_dicts, outbound_dicts, abnormal_dicts, inbound_half_dicts, outbound_half_dicts, abnormal_half_dicts, pod_latency


def get_half_trace(trace_json):  # 获取需要阶段的trace_dict
    # 定义一个trace
    trace_dict = {'traceId': trace_json['traceID']}
    # 定义trace字段
    trace_dict['call'] = []
    trace_dict['timestamp'] = []
    trace_dict['latency'] = []
    trace_dict['http_status'] = []
    trace_dict['svc'] = []
    trace_dict['call_instance'] = []

    for span_json in trace_json['spans']:
        # 获取基本信息
        trace_dict['timestamp'].append(span_json['startTime'])
        trace_dict['latency'].append(span_json['duration'])
        # 根据状态码判断是否正常
        for tag in span_json['tags']:
            if tag['key'] == 'http.status_code':
                trace_dict['http_status'].append(tag['value'])

    return trace_dict
