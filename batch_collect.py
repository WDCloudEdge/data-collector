import sys

import pkl_concat
from Config import Config
import MetricCollector
from handler import Trace
import time


if __name__ == "__main__":
    # 要收集的命名空间
    namespaces = ['bookinfo', 'hipster', 'hipster2', 'cloud-sock-shop', 'horsecoder-test']
    config = Config()

    # 多次收集数据所需要的参数
    batch_name = "bookinfo-multi-pod"   # 批次内容
    deployment_name = "cloud_pod_productpage_productpage-v1-58d88df557-plgf8"  # deployment名
    chaos_name = "cpu"  # 故障类型
    batch_size = 2  # 批次数量
    cycle_time = 900  # 一个周期的时间
    each_batch_end_time = 1020  # 一个批次开始到结束的时间
    first_start_time = "2024-02-06 23:55:00"  # 第一批的开始时间
    # 转换为时
    first_start_time_array = time.strptime(first_start_time, "%Y-%m-%d %H:%M:%S")
    first_start_timestamp = int(time.mktime(first_start_time_array))

    now_timestamp = first_start_timestamp
    end_timestamp = first_start_timestamp + each_batch_end_time

    # 对每个批次进行循环
    for i in range(batch_size):
        config.user = deployment_name + '_' + chaos_name + '_' + str(i + 1)
        print(config.user)

        global_now_time = now_timestamp
        global_end_time = end_timestamp

        now = int(time.time())
        if global_now_time > now:
            sys.exit("begin time is after now time")
        if global_end_time > now:
            global_end_time = now

        for n in namespaces:
            config.namespace = n
            config.svcs.clear()
            config.pods.clear()
            count = 1
            now_time = global_now_time
            end_time = global_end_time
            data_folder = './data/' + '/' + batch_name + '/' + str(config.user) + '/' + config.namespace
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
        data_folder = './data/' + '/' + batch_name + '/' + str(config.user) + '/node'
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

        now_timestamp = now_timestamp + cycle_time
        end_timestamp = end_timestamp + cycle_time

    # 需要更改的部分
    # config.user = "normal-240119-0400-1130"
    # now_time_string = "2024-01-17 00:00:00"
    # end_time_string = "2024-01-17 11:00:00"
    #
    # now_time_array = time.strptime(now_time_string, "%Y-%m-%d %H:%M:%S")
    # end_time_array = time.strptime(end_time_string, "%Y-%m-%d %H:%M:%S")
    # global_now_time = int(time.mktime(now_time_array))
    # global_end_time = int(time.mktime(end_time_array))
    # now = int(time.time())
    # if global_now_time > now:
    #     sys.exit("begin time is after now time")
    # if global_end_time > now:
    #     global_end_time = now
    #
    # folder = '.'
    # for n in namespaces:
    #     config.namespace = n
    #     config.svcs.clear()
    #     config.pods.clear()
    #     count = 1
    #     now_time = global_now_time
    #     end_time = global_end_time
    #     data_folder = './data/' + str(config.user) + '/' + config.namespace
    #     while now_time < end_time:
    #         config.start = int(round(now_time))
    #         config.end = int(round(now_time + config.duration))
    #         if config.end > end_time:
    #             config.end = end_time
    #         print('第' + str(count) + '次获取 [' + config.namespace + '] 数据')
    #         if count == 1:
    #             is_header = True
    #         else:
    #             is_header = False
    #         MetricCollector.collect(config, data_folder, is_header)
    #         Trace.collect(config, data_folder)
    #         now_time += config.duration + config.step
    #         config.pods.clear()
    #         count += 1
    #     # 将trace数据合并，写入到原文件中
    #     pkl_concat.data_concat(data_folder, data_folder)
    # data_folder = './data/' + str(config.user) + '/node'
    # count = 1
    # now_time = global_now_time
    # end_time = global_end_time
    # while now_time < end_time:
    #     config.start = int(round(now_time))
    #     config.end = int(round(now_time + config.duration))
    #     if config.end > end_time:
    #         config.end = end_time
    #     if count == 1:
    #         is_header = True
    #     else:
    #         is_header = False
    #     print('第' + str(count) + '次获取 [' + 'node' + '] 数据')
    #     MetricCollector.collect_node(config, data_folder, is_header)
    #     now_time += config.duration + config.step
    #     count += 1
