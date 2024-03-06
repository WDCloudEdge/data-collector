import json
import os
import time
from kubernetes import client, config
from threading import Thread
import atexit

config.kube_config.load_kube_config(config_file='config.yaml')
v1 = client.CoreV1Api()


def watch_pods_lifecycle_specified_namespace(specified_namespace, file_path, interval=5):
    time_format = "%Y-%m-%d %H:%M:%S"
    data = {}
    deleted = {}
    atexit.register(dump_shutdown_hook, data, file_path)
    while True:
        api_response = v1.list_namespaced_pod(specified_namespace, pretty=True)
        for pod in api_response.items:
            pod_name = pod.metadata.name
            if pod_name in deleted:
                continue
            if pod_name not in data:
                data[pod_name] = {}
                data[pod_name]["pod_name"] = pod_name
                data[pod_name]['creation_timestamp'] = pod.metadata.creation_timestamp.strftime(time_format)
                data[pod_name]['conditions'] = {}
                if pod.metadata.owner_references:
                    data[pod_name]['owner_kind'] = pod.metadata.owner_references[0].kind
                    data[pod_name]['owner_name'] = pod.metadata.owner_references[0].name
                else:
                    data[pod_name]['owner_kind'] = None
                    data[pod_name]['owner_name'] = None

            if pod.status.conditions:
                for condition in pod.status.conditions:
                    if condition.type in ["Initialized", "PodScheduled"]:
                        if condition.type not in data[pod_name]:
                            data[pod_name][condition.type] = condition.last_transition_time.strftime(time_format)
                        continue
                    if condition.type not in data[pod_name]['conditions'] or condition.status != \
                            data[pod_name]['conditions'][condition.type]:
                        data[pod_name]['conditions'][condition.type] = condition.status
                        if condition.status == "True":
                            if condition.type not in data[pod_name]:
                                data[pod_name][condition.type] = []
                            data[pod_name][condition.type].append(
                                {'start': condition.last_transition_time.strftime(time_format), 'end': None,
                                 'duration': None})
                        else:
                            if condition.type in data[pod_name]:
                                data[pod_name][condition.type][-1]['end'] = condition.last_transition_time.strftime(
                                    time_format)
                                data[pod_name][condition.type][-1]['duration'] = int(
                                    condition.last_transition_time.timestamp()) - int(time.mktime(
                                    time.strptime(data[pod_name][condition.type][-1]['start'], time_format))) - int(
                                    time.time() - time.mktime(time.gmtime()))
            if 'container_started_at' not in data[pod_name]:
                data[pod_name]['container_started_at'] = {}
            if pod.status.container_statuses:
                for container in pod.status.container_statuses:
                    if container.name not in data[pod_name]['container_started_at']:
                        data[pod_name]['container_started_at'][container.name] = []
                    if container.state.running and hasattr(container.state.running, 'started_at'):
                        if not data[pod_name]['container_started_at'][
                            container.name] or container.state.running.started_at.strftime(time_format) != \
                                data[pod_name]['container_started_at'][container.name][-1]:
                            data[pod_name]['container_started_at'][container.name].append(
                                container.state.running.started_at.strftime(time_format))

            if pod.metadata.deletion_timestamp is not None:
                data[pod_name]['deletion_timestamp'] = pod.metadata.deletion_timestamp.strftime(time_format)
                data[pod_name]['cost'] = {}
                for key in data[pod_name]['conditions']:
                    if key in data[pod_name] and data[pod_name][key]:
                        data[pod_name]['cost'][key + "_cost_total"] = int(
                            time.mktime(time.strptime(data[pod_name][key][0]['start'], time_format))) - int(
                            time.mktime(time.strptime(data[pod_name]['creation_timestamp'], time_format)))
                condition = ['PodScheduled', 'Initialized', 'ContainersReady', 'Ready']
                for i in range(1, 4):
                    if condition[i - 1] in data[pod_name] and data[pod_name][condition[i - 1]] != [] and condition[i] in \
                            data[pod_name] and data[pod_name][condition[i]] != []:
                        start_time = data[pod_name][condition[i - 1]] if condition[i - 1] in ["Initialized",
                                                                                              "PodScheduled"] else \
                            data[pod_name][condition[i - 1]][0]['start']
                        end_time = data[pod_name][condition[i]] if condition[i] in ["Initialized", "PodScheduled"] else \
                            data[pod_name][condition[i]][0]['start']
                        print(start_time)
                        print(end_time)
                        data[pod_name]['cost'][condition[i - 1] + "-" + condition[i]] = int(
                            time.mktime(time.strptime(end_time, time_format))) - int(
                            time.mktime(time.strptime(start_time, time_format)))
                if os.path.exists(file_path):
                    with open(file_path, mode="a") as f:
                        f.write(",\n")
                        json.dump(data[pod_name], f, indent=2, sort_keys=False)
                else:
                    if not os.path.exists(folder):
                        os.makedirs(folder)
                    with open(file_path, mode="w") as f:
                        f.write("[")
                        json.dump(data[pod_name], f, indent=2, sort_keys=False)
                data.pop(pod_name)
                print(pod_name + " data collected")
                deleted[pod_name] = True
        time.sleep(interval)


def async_watch_pods_lifecycle_specified_namespace(specified_namespace, file_path, interval=5):
    Thread(target=watch_pods_lifecycle_specified_namespace,
           args=(specified_namespace, file_path, interval)).start()


def dump_shutdown_hook(data, file_path):
    if os.path.exists(file_path):
        with open(file_path, mode="a") as f:
            for pod_name in data:
                f.write(",\n")
                json.dump(data[pod_name], f, indent=2, sort_keys=False)
            f.write("]")
    else:
        if not os.path.exists(folder):
            os.makedirs(folder)
        with open(file_path, mode="w") as f:
            count = 1
            for pod_name in data:
                if count == 1:
                    f.write("[")
                else:
                    f.write(",\n")
                json.dump(data[pod_name], f, indent=2, sort_keys=False)
                count += 1
            f.write("]")


if __name__ == '__main__':
    # namespaces = ['bookinfo', 'hipster', 'hipster2', 'cloud-sock-shop', 'horsecoder-test']
    namespaces = ['bookinfo']
    folder = 'podLifecycle'
    for namespace in namespaces:
        file_path = folder + '/pod-metrics-' + namespace + '.json'
        async_watch_pods_lifecycle_specified_namespace(namespace, file_path)
