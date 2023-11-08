import requests
from Config import Config
import pickle


def collect(config: Config, _dir: str):
    urls = build_trace_urls(config)
    traces_dict = {}
    for url in urls:
        trace_dict = handle(pull(url))
        traces_dict = {**traces_dict, **trace_dict}
    pipe_path = _dir + '/' + 'traces.pkl'
    with open(pipe_path, 'ab') as fw:
        pickle.dump(list(traces_dict.values()), fw)


def build_trace_urls(config: Config):
    '''
        构建每个服务每分钟的trace拉取路径（避免数据量太大）
    '''
    if 'bookinfo' == config.namespace:
        config.svcs.remove('reviews-v1')
        config.svcs.remove('reviews-v2')
        config.svcs.remove('reviews-v3')
        config.svcs.add('reviews')
        svcs = [svc + '.' + config.namespace for svc in config.svcs if 'unknown' not in svc and 'redis' not in svc and 'istio-ingressgateway' not in svc]
    else:
        svcs = [svc + '.' + config.namespace for svc in config.svcs if 'unknown' not in svc and 'redis' not in svc and 'istio-ingressgateway' not in svc]
    urls = ['{}end={}&start={}&limit={}&lookback={}&maxDuration&minDuration&service={}' \
                .format(config.jaeger_url, config.end * 1000000, config.start * 1000000, config.limit, config.lookBack,
                        svc) for
            svc in
            svcs]
    return urls
    # windowsSize = 10
    # start = config.start
    # end = config.start + windowsSize # 10s一个windows
    # windows = []
    # while end <= config.end:
    #     windows.append((start, end))
    #     start += windowsSize
    #     end += windowsSize
    # # 为每一个滑动窗口的服务拼接一个获取地址
    # for window in windows:
    #     urls = ['{}end={}&start={}&limit={}&lookback={}&maxDuration&minDuration&service={}' \
    #         .format(config.jaeger_url, window[1], window[0], config.limit, config.lookBack, svc) for svc in svcs]           
    # return urls


def pull(url):
    '''
        拉取trace
    '''
    response = requests.get(url)
    return response.json()['data']


def handle(trace_jsons):
    '''
        处理原始trace文件
    '''
    traces_dict = {}
    for trace_json in trace_jsons:
        # traceId
        trace_dict = {'traceId': trace_json['traceID']}
        # 解析 span
        trace_dict['call'] = []
        trace_dict['timestamp'] = []
        trace_dict['latency'] = []
        trace_dict['http_status'] = []
        trace_dict['svc'] = []
        trace_dict['call_instance'] = []

        spans_dict = {}
        for span_json in trace_json['spans']:
            spans_dict[span_json['spanID']] = span_json
        for span_json in trace_json['spans']:
            trace_dict['timestamp'].append(span_json['startTime'])
            trace_dict['latency'].append(span_json['duration'])
            [trace_dict['http_status'].append(tag['value']) for tag in span_json['tags'] if
             tag['key'] == 'http.status_code']
            # todo 解决UNKNOWN问题
            node_id = None
            caller_svc = None
            callee_svc = None
            outbound_node_id = None
            for tag in span_json['tags']:
                if 'node_id' == tag['key']:
                    node_id = tag['value']
                if 'upstream_cluster' == tag['key']:
                    if 'inbound' in tag['value']:
                        for ref in span_json['references']:
                            if ref['refType'] == 'CHILD_OF':
                                outbound_span = spans_dict[ref['spanID']]
                                for outbound_tag in outbound_span['tags']:
                                    if 'node_id' == outbound_tag['key']:
                                        outbound_node_id = outbound_tag['value']
                                    if 'istio.canonical_service' == outbound_tag['key']:
                                        caller_svc = outbound_tag['value']
                if 'istio.canonical_service' == tag['key']:
                    callee_svc = tag['value']
            trace_dict['svc'].append(callee_svc)
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


def handle_istio_node_id(node_id: str) -> str:
    s = node_id.split('~')
    if len(s) != 4:
        raise Exception('istio trace node id illegal')
    return s[2] + '-' + s[1]
