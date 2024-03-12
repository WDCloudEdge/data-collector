import sys

import pkl_concat
from Config import Config
import MetricCollector
from handler import Trace, Log
import time
from util.utils import *
import os

if __name__ == "__main__":
    # namespaces = ['bookinfo', 'hipster', 'hipster2', 'cloud-sock-shop', 'horsecoder-test']
    namespaces = ['bookinfo']
    # namespaces = ['horsecoder-test']
    config = Config()


    class Simple:
        def __init__(self, label, begin, end):
            self.label = label
            self.begin = begin
            self.end = end


    def read_label_logs(label_file, simple_list: [Simple]):
        if simple_list is None:
            simple_list = []
        file_path = label_file
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
                # 如果文件为空则跳过
                if not lines:
                    return simple_list
                for line in lines:
                    if 'cpu_' in line or 'mem_' in line or 'net_' in line:
                        root_cause = line.strip()
                        simple = Simple(root_cause, None, None)
                    elif 'start create' in line:
                        begin = line[:18]
                    elif 'finish delete' in line:
                        end = line[:18]
                        simple.begin = timestamp_2_time_string(time_string_2_timestamp(begin) - 360)
                        simple.end = timestamp_2_time_string(time_string_2_timestamp(end) + 360)
                        simple_list.append(simple)
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")


    simple_list: [Simple] = []
    read_label_logs('data/topoChange/label.txt', simple_list)
    for simple in simple_list:
        config.user = simple.label
        now_time_string = simple.begin
        end_time_string = simple.end

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
                MetricCollector.collect(config, os.path.join(data_folder, 'metrics'), is_header)
                Trace.collect(config, os.path.join(data_folder, 'trace'))
                Log.collect(config, os.path.join(data_folder, 'log'))
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
