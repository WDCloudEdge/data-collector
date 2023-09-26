from Config import Config
import handler.Metric as Metric
import handler.Trace as Trace
import handler.Log as Log
import time
import MetricCollector
from handler import Trace

if __name__ == "__main__":
    # config = Config()
    # duration = 60
    # now_time = time.time()
    # folder = '.'
    # fault_type = 'memory-cartservice-8444494f87-868qq'
    # min = 1
    # max = 11
    # for i in range(min, max):
    #     config.start = int(round((now_time - duration) * (10 ** 6)))
    #     config.end = int(round(now_time * (10 ** 6)))
    #     print('第' + str(i) +'次获取数据')
    #     Metric.collect(config, fault_type, i)
    #     now_time += duration
    #     if i != max - 1: time.sleep(duration)
    # Metric.collect(config)
    # Trace.collect(config)
    # Log.collect(config)
    config = Config()
    now_time = config.end
    folder = '.'
    data_folder = './data/' + config.namespace + '/' + str(config.user)
    min = 1
    max = 2
    for i in range(min, max):
        config.start = int(round((now_time - config.duration)))
        config.end = int(round(now_time))
        print('第' + str(i) + '次获取数据')
        MetricCollector.collect(config, data_folder)
        now_time += config.duration
        Trace.collect(config, data_folder)
        config.pods.clear()
        # if i != max - 1:
        #     time.sleep(config.duration)
