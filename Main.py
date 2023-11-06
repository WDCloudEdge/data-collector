from Config import Config
import handler.Metric as Metric
import handler.Trace as Trace
import handler.Log as Log
import time
import MetricCollector
from handler import Trace

if __name__ == "__main__":
    namespaces = ['bookinfo', 'hipster', 'hipster2', 'sock-shop', 'horsecoder-test', 'horsecoder-minio']
    config = Config()
    global_now_time = 1697777100
    global_end_time = 1697777100
    folder = '.'
    for _, n in namespaces:
        config.namespace = n
        config.svcs.clear()
        config.pods.clear()
        count = 1
        now_time = global_now_time
        end_time = global_end_time
        data_folder = './data/' + config.namespace + '/' + str(config.user)
        while now_time < end_time:
            config.start = int(round(now_time))
            config.end = int(round(now_time + config.duration))
            print('第' + str(count) + '次获取 [' + config.namespace + '] 数据')
            MetricCollector.collect(config, data_folder)
            now_time += config.duration + 1
            Trace.collect(config, data_folder)
            config.pods.clear()
            count += 1
