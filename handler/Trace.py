import requests
from Config import Config
import pickle

def collect(config: Config, _dir: str):
    svcs = [svc for svc in config.svcs if
            'unknown' not in svc and 'redis' not in svc and 'istio-ingressgateway' not in svc]
    # urls = build_trace_urls(config)
    normal_dicts = {}
    inbound_dicts = {}
    outbound_dicts = {}
    abnormal_dicts = {}

    for svc in svcs:
        url = build_trace_url(config, svc + '.' + config.namespace)
        # trace_dict = handle(pull(url))
        normal_dict, inbound_dict, outbound_dict, abnormal_dict = handle_traces_2(pull(url), svc)
        normal_dicts = {**normal_dicts, **normal_dict}
        inbound_dicts = {**inbound_dicts, **inbound_dict}
        outbound_dicts = {**outbound_dicts, **outbound_dict}
        abnormal_dicts = {**abnormal_dicts, **abnormal_dict}
    normal_path = _dir + '/' + 'normal.pkl'
    inbound_path = _dir + '/' + 'inbound.pkl'
    outbound_path = _dir + '/' + 'outbound.pkl'
    abnormal_path = _dir + '/' + 'abnormal.pkl'
    with open(normal_path, 'ab') as fw:
        pickle.dump(list(normal_dicts.values()), fw)
    with open(inbound_path, 'ab') as fw:
        pickle.dump(list(inbound_dicts.values()), fw)
    with open(outbound_path, 'ab') as fw:
        pickle.dump(list(outbound_dicts.values()), fw)
    with open(abnormal_path, 'ab') as fw:
        pickle.dump(list(abnormal_dicts.values()), fw)


def build_trace_url(config: Config, svc):
    url = '{}end={}&start={}&limit={}&lookback={}&maxDuration&minDuration&service={}' \
                .format(config.jaeger_url, config.end * 1000000, config.start * 1000000, config.limit, config.lookBack,
                        svc)
    return url

def build_trace_urls(config: Config):
    '''
        构建每个服务每分钟的trace拉取路径（避免数据量太大）
    '''
    svcs = [svc + '.' + config.namespace for svc in config.svcs if 'unknown' not in svc and 'redis' not in svc and 'istio-ingressgateway' not in svc]
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


def handle(trace_jsons):
    '''
            处理原始trace文件
    '''
    traces_dict = {}
    for trace_json in trace_jsons:
        # 定义一个trace
        trace_dict = {'traceId': trace_json['traceID']}
        # 定义trace字段
        trace_dict['call'] = []
        trace_dict['timestamp'] = []
        trace_dict['latency'] = []
        trace_dict['http_status'] = []
        trace_dict['svc'] = []
        trace_dict['call_instance'] = []

        # 获取trace中的所有span
        spans_dict = {}
        for span_json in trace_json['spans']:
            spans_dict[span_json['spanID']] = span_json
        # 遍历trace中的所有span，处理数据
        for span_json in trace_json['spans']:
            # 获取基本信息
            trace_dict['timestamp'].append(span_json['startTime'])
            trace_dict['latency'].append(span_json['duration'])
            [trace_dict['http_status'].append(tag['value']) for tag in span_json['tags'] if
             tag['key'] == 'http.status_code']
            # 设置变量
            node_id = None  # 节点id
            caller_svc = None  # 调用者服务名
            callee_svc = None  # 被调用者服务名
            outbound_node_id = None  # 出边的节点id
            # 遍历span中的所有tag
            for tag in span_json['tags']:
                # 找到节点id
                if 'node_id' == tag['key']:
                    node_id = tag['value']
                # 查看是出边还是入边（调用者还是被调用者）
                if 'upstream_cluster' == tag['key']:
                    if 'inbound' in tag['value']:
                        # 如果是入边（被调用者），则找到相应的出边的span（调用者信息）
                        for ref in span_json['references']:
                            if ref['refType'] == 'CHILD_OF':
                                outbound_span = spans_dict[ref['spanID']]
                                for outbound_tag in outbound_span['tags']:
                                    # 找到出边的节点id
                                    if 'node_id' == outbound_tag['key']:
                                        outbound_node_id = outbound_tag['value']
                                    # 找到调用者的服务名
                                    if 'istio.canonical_service' == outbound_tag['key']:
                                        caller_svc = outbound_tag['value']
                                        # 如果服务名是unkown，则将其替换
                                        if caller_svc == 'unknown':
                                            caller_svc = 'istio-ingressgateway'
                    # 找到只有outbound或inbound的span，则为系统交界区

                # 找到被调用者的服务名
                if 'istio.canonical_service' == tag['key']:
                    callee_svc = tag['value']
            # 将被调用者的服务名加入到trace中
            trace_dict['svc'].append(callee_svc)
            # 添加调用关系
            if node_id is not None and outbound_node_id is not None:
                trace_dict['call'].append((caller_svc, callee_svc))
                trace_dict['call_instance'].append(
                    (handle_istio_node_id(outbound_node_id), handle_istio_node_id(node_id)))
                # try:
                #     trace_dict['call'].append((
                #         spans_dict[ref['spanID']]['operationName'].split('.')[0],
                #         span_json['operationName'].split('.')[0])
                #     )
                # except:
                #     # 存在断链（未能接收到某个节点的span数据）
                #     trace_dict['call'].append((
                #         None, span_json['operationName'].split('.')[0])
                #     )

        traces_dict[trace_json['traceID']] = trace_dict
    return traces_dict

'''
    更改后的trace处理方法
'''
def handle_traces(trace_jsons):
    traces_dict = {}
    inbounds_dict = {}
    outbounds_dict = {}
    exceptions_dict = {}
    # 定义一个trace
    for trace_json in trace_jsons:
        # 定义一个trace
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
            [trace_dict['http_status'].append(tag['value']) for tag in span_json['tags'] if
             tag['key'] == 'http.status_code']

            # 设置变量
            node_id = None  # 节点id
            caller_svc = None  # 调用者服务名
            callee_svc = None  # 被调用者服务名
            outbound_node_id = None  # 出边的节点id

            # 遍历span中的所有tag
            for tag in span_json['tags']:
                # 找到节点id
                if 'node_id' == tag['key']:
                    node_id = tag['value']
                # 找到被调用者的服务名（如果你是的话）
                if 'istio.canonical_service' == tag['key']:
                    callee_svc = tag['value']

            # 查看是出边还是入边
            for tag in span_json['tags']:
                # 如果是入边，判断是否有引用
                if 'upstream_cluster' == tag['key']:
                    if 'inbound' in tag['value']:
                        # 移除该span的标志位
                        span_flag.remove(span_json['spanID'])
                        # 将其添加到inbounds_dict中
                        # inbound_dict = get_inbound_dict(span_json)
                        # inbounds_dict[span_json['spanID']] = inbound_dict
                        # 有引用，则找到相应的出边，绑定成一对
                        if span_json['references'] != []:
                            for ref in span_json['references']:
                                if ref['refType'] == 'CHILD_OF':
                                    # 找到出边的span
                                    outbound_span = spans_dict[ref['spanID']]
                                    # 移除该span的标志位
                                    if ref['spanID'] in span_flag:
                                        span_flag.remove(ref['spanID'])
                                    # 将其添加到outbounds_dict中
                                    # outbound_dict = get_outbound_dict(outbound_span)
                                    # outbounds_dict[outbound_span['spanID']] = outbound_dict
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
                        # 如果没有引用，则用特定字符替代
                        else:
                            caller_svc = 'OTHER_SYSTEM'
                            outbound_node_id = 'OTHER_NODE'
                        # 将调用关系添加到trace中
                        trace_dict['svc'].append(callee_svc)
                        # 添加调用关系
                        if node_id is not None:
                            trace_dict['call'].append((caller_svc, callee_svc))
                            trace_dict['call_instance'].append(
                                (handle_istio_node_id(outbound_node_id), handle_istio_node_id(node_id)))

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
                        # 将其添加到outbounds_dict中
                        # outbound_dict = get_outbound_dict(span)
                        # outbounds_dict[span['spanID']] = outbound_dict
                        # 将其移除标志位
                        span_flag.remove(span['spanID'])
                        trace_dict['svc'].append('OTHER_SERVICE')
                        # 添加调用关系
                        if node_id is not None:
                            trace_dict['call'].append((caller_svc, 'OTHER_SYSTEM'))
                            trace_dict['call_instance'].append(
                                (handle_istio_node_id(node_id), handle_istio_node_id('OTHER_NODE')))

        # 再次进行遍历，将这些span加入到exception_dict
        for span_id in span_flag:
            span = spans_dict[span_id]
            # exception_dict = get_exception_dict(span)
            # exceptions_dict[span['spanID']] = exception_dict

        # 如果是异常情况，不将其加入到trace信息中
        if trace_dict['call'] == []:
            continue
        # 将这条trace加入
        traces_dict[trace_json['traceID']] = trace_dict
    return traces_dict, inbounds_dict, outbounds_dict, exceptions_dict


def handle_traces_2(trace_jsons, current_svc):
    # 需要构建的trace列表
    normal_dicts = {}
    inbound_dicts = {}
    outbound_dicts = {}
    abnormal_dicts = {}
    # 遍历所有trace
    for trace_json in trace_jsons:

        # 定义标志位
        is_abnormal = False
        is_inbound = False
        is_outbound = False

        # 定义一个trace
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
                                        if current_svc != callee_svc:
                                            # 证明目前该方是调用者，为outbound
                                            is_outbound = True
                                        else:
                                            is_inbound = True

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
                            trace_dict['timestamp'].insert(len(trace_dict['timestamp']) - 1, -1)
                            trace_dict['latency'].insert(len(trace_dict['latency']) - 1, -1)
                            trace_dict['http_status'].insert(len(trace_dict['latency']) - 1, -1)
                        # 为出边，改变标志位
                        is_outbound = True

        # 再次进行遍历，判断是否为异常状况
        if span_flag != []:
            is_abnormal = True

        # 根据标志位判断是否加入各trace信息中
        if is_abnormal == True:
            abnormal_dicts[trace_json['traceID']] = trace_dict
        else:
            normal_dicts[trace_json['traceID']] = trace_dict
        if is_inbound == True:
            inbound_dicts[trace_json['traceID']] = trace_dict
        if is_outbound == True:
            outbound_dicts[trace_json['traceID']] = trace_dict
    return normal_dicts, inbound_dicts, outbound_dicts, abnormal_dicts

