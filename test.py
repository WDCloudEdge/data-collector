import pickle


# 由于pickle每次序列化生成的字符串有独立头尾，pickle.load()只会按顺序读取一个完整结果
# 因此需要反复load知道抛出异常为止
def list_load(path):
    data = []
    with open(path, 'rb') as f:
        while True:
            try:
                data.extend(pickle.load(f))
            except EOFError:
                break
    return data


def dict_load(path):
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
    return data


# data = list_load('data/normal-240110-2130-240111-1130/concat/bookinfo/normal.pkl')
# print(len(data))

data = dict_load('data/normal-240110-2130-240111-1130/concat/bookinfo/trace_net_latency.pkl')
for key in data.keys():
    print(key + ': ',  data[key])






