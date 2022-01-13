import pickle


def data_load(path):
    f = open(path, 'rb')
    return pickle.load(f)

logs = data_load('data/log/productcatalogservice-6857b8d67c-gc5v4.pkl')
print(logs)