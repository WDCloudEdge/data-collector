import pickle
import os
def list_concat(path, save_path):
    data = []
    # 不断读取pkl文件，并将其合并为一个list
    with open(path, 'rb') as f:
        while True:
            try:
                data.extend(pickle.load(f))
            except EOFError:
                break
    with open(save_path, 'wb') as fw:
        pickle.dump(data, fw)
    return data

def dict_concat(path, save_path):
    data = {}
    with open(path, 'rb') as f:
        while True:
            try:
                tmp_dict = pickle.load(f)
                for key in tmp_dict.keys():
                    if key not in data.keys():
                        data[key] = []
                    value = tmp_dict[key]
                    data[key] = data[key] + value
            except EOFError:
                break
    with open(save_path, 'wb') as fw:
        pickle.dump(data, fw)


def data_concat(dir_path, save_dir_path):
    if not os.path.exists(dir_path):
        return
    if not os.path.exists(save_dir_path):
        os.makedirs(save_dir_path)
    list_concat(dir_path + '/normal.pkl', save_dir_path + '/normal.pkl')
    list_concat(dir_path + '/abnormal.pkl', save_dir_path + '/abnormal.pkl')
    list_concat(dir_path + '/abnormal_half.pkl', save_dir_path + '/abnormal_half.pkl')
    list_concat(dir_path + '/inbound.pkl', save_dir_path + '/inbound.pkl')
    list_concat(dir_path + '/inbound_half.pkl', save_dir_path + '/inbound_half.pkl')
    list_concat(dir_path + '/outbound.pkl', save_dir_path + '/outbound.pkl')
    list_concat(dir_path + '/outbound_half.pkl', save_dir_path + '/outbound_half.pkl')
    dict_concat(dir_path + '/trace_net_latency.pkl', save_dir_path + '/trace_net_latency.pkl')
    dict_concat(dir_path + '/trace_pod_latency.pkl', save_dir_path + '/trace_pod_latency.pkl')


if __name__ == '__main__':
    namespaces = ['bookinfo', 'hipster', 'hipster2', 'cloud-sock-shop', 'horsecoder-test']
    # namespaces = ['bookinfo']
    dir_path = 'data/normal-240110-2130-240111-1130/'
    save_dir_path = 'data/normal-240110-2130-240111-1130/concat/'
    for namespace in namespaces:
        data_concat(dir_path + namespace, save_dir_path + namespace)
