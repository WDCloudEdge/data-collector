import pickle


def data_load(path):
    f = open(path, 'rb')
    return pickle.load(f)

traces = data_load('data/traces.pkl')
print(traces[10])