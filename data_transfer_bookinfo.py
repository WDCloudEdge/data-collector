import os.path

import numpy as np
import pandas as pd

def parse_call_2_service_graph(dir):
    df = pd.read_csv(
    os.path.join(dir, "call.csv"))


    def parse_call(col):
        # 去掉 &p50
        col = col.replace("&p90", "")

        # 按 _ 分割
        parts = col.split("_")

        if len(parts) != 2:
            return None, None

        src, dst = parts

        # 去掉 service 后缀（统一命名）
        src = src.replace("service", "")
        dst = dst.replace("service", "")

        return src, dst


    # 你的数据是 tab 分隔
    df = df.iloc[:, 1:]
    # 每一列取平均值（忽略空值）

    col_avg = df.mean(skipna=True)

    services = set()
    edges = []

    for col, val in col_avg.items():
        if "p90" not in col:
            continue
        src, dst = parse_call(col)

        if src is None:
            continue

        services.add(src)
        services.add(dst)

        edges.append((src, dst, val))

    services = sorted(list(services))
    services.remove("unknown")
    services.remove("openclaw-gateway")

    # 初始化矩阵
    matrix = pd.DataFrame(0, index=services, columns=services)

    # 填充
    for src, dst, val in edges:
        matrix.loc[src, dst] = val

    target_services = ['details-v1', 'productpage-v1', 'ratings-v1', 'reviews-v1', 'reviews-v2', 'reviews-v3' , 'openclaw-gateway']

    matrix = matrix.reindex(index=target_services, columns=target_services, fill_value=0)

    matrix.to_csv(os.path.join(dir, "ServiceGraph.csv"))


def parse_svc_metric_2_service_resource(dir):
    df = pd.read_csv(
        os.path.join(dir, "svc_metric.csv"))


    # 你的数据是 tab 分隔
    df = df[[col for col in df.columns if col != "timestamp"]]
    # 每一列取平均值（忽略空值）

    col_avg = df.replace(0, np.nan).mean(skipna=True)

    services = set()
    service_cpu_map = {}
    service_mem_map = {}

    for col, val in col_avg.items():
        if not "&mem_usage" == col[-10:] and not "&cpu_usage" == col[-10:]:
            continue
        src = col[:-10].replace("service", "")

        if src is None:
            continue

        services.add(src)
        if "&mem_usage" == col[-10:]:
            service_mem_map[src] = val
        elif "&cpu_usage" == col[-10:]:
            service_cpu_map[src] = val

    services = sorted(list(services))

    columns = ["cpu", "mem"]
    # 初始化矩阵
    matrix = pd.DataFrame(0, index=services, columns=columns)

    # 填充
    for src, val in service_mem_map.items():
        matrix.loc[src, "mem"] = val

    for src, val in service_cpu_map.items():
        matrix.loc[src, "cpu"] = val * 1000

    # matrix = matrix.reindex(columns=columns, fill_value=0)

    matrix.to_csv(os.path.join(dir, "ServiceResource.csv"))

if __name__ == '__main__':
    # dir = "data/10user-5min/bookinfo/metrics"
    dir = "data/bookinfo/10user-5min/bookinfo/metrics"
    parse_call_2_service_graph(dir)
    parse_svc_metric_2_service_resource(dir)
