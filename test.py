import pickle


def data_load(path):
    f = open(path, 'rb')
    return pickle.load(f)

logs = data_load('data/trace/traces.pkl')
print(logs[10])