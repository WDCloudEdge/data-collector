import sys
from Config import Config
import MetricCollector
from handler import Trace
import time


if __name__ == "__main__":
    # namespaces = ['bookinfo', 'hipster', 'hipster2', 'cloud-sock-shop', 'horsecoder-test']
    namespaces = ['bookinfo']
    config = Config()
    # 需要更改的部分
    config.user = "test"
    now_time_string = "2024-01-11 00:00:00"
    end_time_string = "2024-01-11 00:30:00"

    now_time_array = time.strptime(now_time_string, "%Y-%m-%d %H:%M:%S")
    end_time_array = time.strptime(end_time_string, "%Y-%m-%d %H:%M:%S")
    global_now_time = int(time.mktime(now_time_array))
    global_end_time = int(time.mktime(end_time_array))
    now = int(time.time())
    if global_now_time > now:
        sys.exit("begin time is after now time")
    if global_end_time > now:
        global_end_time = now

    folder = '.'
    for n in namespaces:
        config.namespace = n
        config.svcs.clear()
        config.pods.clear()
        count = 1
        now_time = global_now_time
        end_time = global_end_time
        data_folder = './data/' + str(config.user) + '/' + config.namespace
        while now_time < end_time:
            config.start = int(round(now_time))
            config.end = int(round(now_time + config.duration))
            if config.end > end_time:
                config.end = end_time
            print('第' + str(count) + '次获取 [' + config.namespace + '] 数据')
            if count == 1:
                is_header = True
            else:
                is_header = False
            MetricCollector.collect(config, data_folder, is_header)
            Trace.collect(config, data_folder)
            now_time += config.duration + config.step
            config.pods.clear()
            count += 1
        # 将trace数据合并，写入到原文件中
        pkl_concat.data_concat(data_folder, data_folder)
    data_folder = './data/' + str(config.user) + '/node'
    count = 1
    now_time = global_now_time
    end_time = global_end_time
    while now_time < end_time:
        config.start = int(round(now_time))
        config.end = int(round(now_time + config.duration))
        if config.end > end_time:
            config.end = end_time
        if count == 1:
            is_header = True
        else:
            is_header = False
        print('第' + str(count) + '次获取 [' + 'node' + '] 数据')
        MetricCollector.collect_node(config, data_folder, is_header)
        now_time += config.duration + config.step
        count += 1
