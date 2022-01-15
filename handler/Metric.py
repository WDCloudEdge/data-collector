from Config import Config
import requests
import pandas as pd

'''
    指标收集
'''


def collect(config: Config):
    # 收集调用关系
    call_df = get_call(config)
    # 收集调用与延时
    latency_df, pod_latency_df = get_latency(config)
    # 收集ctn CPU, memory, network, qps
    pod_metric_df = pod_metric(config)
    # 收集节点CPU, memory, network
    node_metric_df = node_metric(config)

    metric_df = pd.concat([pod_metric_df, node_metric_df], axis=1)

    call_df.to_csv('data/metric/call.csv')
    latency_df.to_csv('data/metric/edge_latency.csv')
    pod_latency_df.to_csv('data/metric/pod_latency.csv')
    metric_df.to_csv('data/metric/metric.csv')


# 收集调用关系
def get_call(config: Config):
    call_df = pd.DataFrame(columns=['source', 'destination'])

    prom_sql = 'sum(istio_tcp_received_bytes_total{destination_workload_namespace=\"%s\"}) by (source_workload, destination_workload)' % config.namespace
    results = execute(config, prom_sql)

    prom_sql = 'sum(istio_requests_total{destination_workload_namespace=\"%s\"}) by (source_workload, destination_workload)' % config.namespace
    results = results + execute(config, prom_sql)

    for result in results:
        metric = result['metric']
        source = metric['source_workload']
        destination = metric['destination_workload']
        config.svcs.add(source)
        config.svcs.add(destination)
        call_df = call_df.append({'source': source, 'destination': destination}, ignore_index=True)

    prom_sql = 'sum(container_cpu_usage_seconds_total{namespace=\"%s\", container!~\'POD|istio-proxy\'}) by (instance, pod)' % config.namespace
    results = execute(config, prom_sql)

    for result in results:
        metric = result['metric']
        if 'pod' in metric:
            source = metric['pod'].split('-')[0]
            destination = metric['instance']
            call_df = call_df.append({'source': source, 'destination': destination}, ignore_index=True)

    return call_df


# 收集调用和延时
def get_latency(config: Config):
    edge_latency_df = pd.DataFrame()
    pod_latency_df = pd.DataFrame()

    # 获取 P50，P90，P99 延时
    prom_50_sql = 'histogram_quantile(0.50, sum(irate(istio_request_duration_milliseconds_bucket{reporter=\"destination\", destination_workload_namespace=\"%s\"}[1m])) by (destination_workload, source_workload, pod_name, le))' % config.namespace
    prom_90_sql = 'histogram_quantile(0.90, sum(irate(istio_request_duration_milliseconds_bucket{reporter=\"destination\", destination_workload_namespace=\"%s\"}[1m])) by (destination_workload, source_workload, pod_name, le))' % config.namespace
    prom_99_sql = 'histogram_quantile(0.99, sum(irate(istio_request_duration_milliseconds_bucket{reporter=\"destination\", destination_workload_namespace=\"%s\"}[1m])) by (destination_workload, source_workload, pod_name, le))' % config.namespace
    responses_50 = execute(config, prom_50_sql)
    responses_90 = execute(config, prom_90_sql)
    responses_99 = execute(config, prom_99_sql)

    def handle(result, edge_latency_df, pod_latency_df, p_type):
        name = result['metric']['source_workload'] + '_' + result['metric']['destination_workload']
        pod_name = result['metric']['pod_name']
        values = result['values']
        values = list(zip(*values))
        if 'timestamp' not in edge_latency_df:
            timestamp = values[0]
            edge_latency_df['timestamp'] = timestamp
            pod_latency_df['timestamp'] = timestamp
            edge_latency_df['timestamp'] = edge_latency_df['timestamp'].astype('datetime64[s]')
            pod_latency_df['timestamp'] = pod_latency_df['timestamp'].astype('datetime64[s]')
        metric = values[1]
        key = name + '&' + p_type
        edge_latency_df[key] = pd.Series(metric)
        edge_latency_df[key] = edge_latency_df[key].astype('float64') * 1000
        key = pod_name + '&' + p_type
        pod_latency_df[key] = pd.Series(metric)
        pod_latency_df[key] = pod_latency_df[key].astype('float64') * 1000

    [handle(result, edge_latency_df, pod_latency_df, 'p50') for result in responses_50]
    [handle(result, edge_latency_df, pod_latency_df, 'p90') for result in responses_90]
    [handle(result, edge_latency_df, pod_latency_df, 'p99') for result in responses_99]

    return edge_latency_df, pod_latency_df


# 收集pod CPU, memory, network
def pod_metric(config: Config):
    df = pd.DataFrame()
    prom_cpu_sql = 'sum(rate(container_cpu_usage_seconds_total{namespace=\'%s\',container!~\'POD|istio-proxy|\',' \
                   'pod!~\'jaeger.*\'}[1m])* 1000)  by (pod, instance,container)' % config.namespace

    response = execute(config, prom_cpu_sql)
    for result in response:
        pod_name = result['metric']['pod']
        prom_memory_sql = 'sum(container_memory_working_set_bytes{namespace=\'%s\',pod="%s"}) by(pod, instance)  / ' \
                          '1000000' % (
                              config.namespace, pod_name)
        prom_network_sql = 'sum(rate(container_network_transmit_packets_total{namespace=\"%s\", pod="%s"}[1m])) * ' \
                           'sum(rate(container_network_transmit_packets_total{namespace=\"%s\", pod="%s"}[1m]))' % (
                               config.namespace, pod_name, config.namespace, pod_name),
        prom_qps_sql = 'round(sum(irate(istio_requests_total{reporter="destination",' \
                       'destination_workload_namespace=~\"%s\", pod_name="%s"}[5m])) by (destination_workload, source_workload), ' \
                       '0.001)' % (config.namespace, pod_name)

        config.pods.add(pod_name)
        values = result['values']
        values = list(zip(*values))
        if 'timestamp' not in df:
            timestamp = values[0]
            df['timestamp'] = timestamp
            df['timestamp'] = df['timestamp'].astype('datetime64[s]')
        metric = pd.Series(values[1])
        col_name = pod_name + '_cpu'
        df[col_name] = metric
        df[col_name] = df[col_name].astype('float64')

        response = execute(config, prom_memory_sql)
        values = response[0]['values']
        values = list(zip(*values))
        metric = pd.Series(values[1])
        col_name = pod_name + '_memory'
        df[col_name] = metric
        df[col_name] = df[col_name].astype('float64')

        response = execute(config, prom_network_sql)
        values = response[0]['values']
        values = list(zip(*values))
        metric = pd.Series(values[1])
        col_name = pod_name + '_network'
        df[col_name] = metric
        df[col_name] = df[col_name].astype('float64')

        response = execute(config, prom_qps_sql)
        values = response[0]['values']
        values = list(zip(*values))
        metric = pd.Series(values[1])
        col_name = pod_name + '_qps'
        df[col_name] = metric
        df[col_name] = df[col_name].astype('float64')

    return df


# 收集节点指标
def node_metric(config: Config):
    df = pd.DataFrame()
    for node in config.nodes.values():
        prom_sql = 'rate(node_network_transmit_packets_total{device="cni0", instance="%s"}[1m]) / 1000' % node
        response = execute(config, prom_sql)
        values = response[0]['values']
        values = list(zip(*values))
        metric = pd.Series(values[1])
        col_name = '(node)' + node + '_network'
        df[col_name] = metric
        df[col_name] = df[col_name].astype('float64')

        prom_sql = '1-(sum(increase(node_cpu_seconds_total{instance="%s",mode="idle"}[1m]))/sum(increase(' \
                   'node_cpu_seconds_total{instance="%s"}[1m])))' % (
            node, node)
        response = execute(config, prom_sql)
        values = response[0]['values']
        values = list(zip(*values))
        metric = pd.Series(values[1])
        col_name = '(node)' + node + '_cpu'
        df[col_name] = metric
        df[col_name] = df[col_name].astype('float64')

        prom_sql = '(node_memory_MemTotal_bytes{instance="%s"}-(node_memory_MemFree_bytes{instance="%s"}+ ' \
                   'node_memory_Cached_bytes{instance="%s"} + node_memory_Buffers_bytes{instance="%s"})) / ' \
                   'node_memory_MemTotal_bytes{instance="%s"}' % (
            node, node, node, node, node)
        response = execute(config, prom_sql)
        values = response[0]['values']
        values = list(zip(*values))
        metric = pd.Series(values[1])
        col_name = '(node)' + node + '_memory'
        df[col_name] = metric
        df[col_name] = df[col_name].astype('float64')

    return df


# 执行prom_sql查询
def execute(config: Config, prom_sql):
    response = requests.get(config.prom_range_url,
                            params={'query': prom_sql,
                                    'start': config.start / 1e6,
                                    'end': config.end / 1e6,
                                    'step': config.step})
    return response.json()['data']['result']
