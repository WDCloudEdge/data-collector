import requests
from Config import Config
import pickle


def collect(config: Config):
    urls = build_trace_urls(config)
    traces_dict = {}
    for url in urls:
        trace_dict = handle(execute(url))
        traces_dict = {**traces_dict, **trace_dict}
    pipe_path = 'data/trace/traces.pkl'
    with open(pipe_path, 'wb') as fw:
        pickle.dump(list(traces_dict.values()), fw)


def build_trace_urls(config: Config):
    """
        构建每个服务每分钟的trace拉取路径（避免数据量太大）
    """
    # svcs = [svc + '.' + config.namespace for svc in config.svcs]
    urls = ['{}end={}&start={}&limit={}&lookback={}&maxDuration&minDuration&service={}' \
                .format(config.jaeger_url, config.end, config.start, config.limit, config.lookBack, svc) for svc in
            config.svcs]
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


def execute(url):
    """
        拉取trace
    """
    response = requests.get(url)
    return response.json()['data']


def handle(trace_jsons):
    """
        处理原始trace文件
    """
    traces_dict = {}
    for trace_json in trace_jsons:
        # traceId
        trace_dict = {'traceId': trace_json['traceID'], 'call': [], 'timestamp': [], 'latency': [],
                      'svcs': [], 'pods': [], 'status_code': [], 'http_status': ''}
        # processes
        processes = trace_json['processes']
        # 解析 span
        spans_dict = {}
        for span_json in trace_json['spans']:
            spans_dict[span_json['spanID']] = span_json
        for span_json in trace_json['spans']:
            trace_dict['timestamp'].append(span_json['startTime'])
            trace_dict['latency'].append(span_json['duration'])
            trace_dict['svcs'].append(processes[span_json['processID']]['serviceName'])
            for tag in span_json['tags']:
                if tag['key'] == 'status.code':
                    trace_dict['status_code'].append(tag['value'])
                elif tag['key'] == 'http.status_code':
                    trace_dict['http_status'] = tag['value']

            for ref in span_json['references']:
                try:
                    trace_dict['call'].append((
                        processes[spans_dict[ref['spanID']]['processID']]['serviceName'],
                        processes[span_json['processID']]['serviceName']
                    ))
                except:
                    # 存在断链（未能接收到某个节点的span数据）
                    trace_dict['call'].append((
                        None, processes[span_json['processID']]['serviceName']
                    ))

        traces_dict[trace_json['traceID']] = trace_dict
    return traces_dict


def get_pod_name(span_json, processes):
    for tag in processes[span_json['processID']]['tags']:
        if tag['key'] == 'name':
            return tag['value']
