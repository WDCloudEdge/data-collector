import time
from threading import Thread

import numpy as np
from kubernetes import client, config, watch
from util.PrometheusClient import PrometheusClient
from Config import Config
import pandas as pd

c = Config()
prom_util = PrometheusClient(c)
specified_namespace = c.namespace
config.load_kube_config()
prom_url = "http://47.99.240.112:31222/api/v1/query"


def pull_and_store(pod_name, reason, delete_time):
    metrics = ['kube_pod_created', 'kube_pod_status_scheduled_time', 'kube_pod_start_time',
               'kube_pod_status_initialized_time',
               'kube_pod_status_container_ready_time', 'kube_pod_status_ready_time', 'kube_pod_deletion_timestamp']
    extra_metrics = ['created-scheduled', 'scheduled_start', 'start_initialized', 'initialized_ready', 'delete_cost']
    prom_sql = 'last_over_time(kube_pod_deletion_timestamp{pod="%s"}[10m])' % pod_name

    cnt = 1
    while not prom_util.execute_prom(prom_url, prom_sql):
        time.sleep(5 * cnt)
        cnt += 1
        if cnt > 15:
            print("Collect_data_from_prometheus_error: " + pod_name)
            return

    datas = [pod_name]
    for metric in metrics:
        prom_sql = 'last_over_time(%s{pod="%s"}[10m])' % (metric, pod_name)
        response = prom_util.execute_prom(prom_url, prom_sql)
        if response != []:
            datas.append(int(response[0]['value'][1]))
        else:
            datas.append(0)
            print("Empty metric: " + pod_name + " " + metric)
    for i in range(1, 5):
        if datas[i] != 0 and datas[i + 1] != 0:
            datas.append(datas[i + 1] - datas[i])
        else:
            datas.append(-1)
    datas.append(datas[7] - delete_time)
    df = pd.DataFrame(data=np.array(datas).reshape(1, 13), columns=["pod_name"] + metrics + extra_metrics)
    with open('pod-metrics.csv', mode="a") as f:
        df.to_csv(f, header=f.tell() == 0, index=False)
    print("Pod metrics collected: " + pod_name)


v1 = client.CoreV1Api()
w = watch.Watch()
end_time = time.time() + 3
for event in w.stream(v1.list_namespaced_event, specified_namespace):
    # for event in w.stream(v1.list_event_for_all_namespaces):
    if time.time() < end_time:
        continue
    print(event)
    if event['object'].reason == "Killing":
        pod_name = event['object'].involved_object.name
        print("Pod " + event['object'].reason + " detected: " + pod_name)
        Thread(target=pull_and_store,
               args=(pod_name, event['object'].reason, int(event['object'].first_timestamp.timestamp()))).start()
