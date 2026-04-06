#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查看 .pkl（pickle）文件内容。pickle 是二进制格式，不能用文本编辑器直接阅读。

用法:
  python3 inspect_pkl.py path/to/file.pkl
  python3 inspect_pkl.py path/to/file.pkl --head 5
  python3 inspect_pkl.py path/to/file.pkl --json       # 缩进 JSON 预览（嵌套 dict 更易读）
  python3 inspect_pkl.py path/to/file.pkl --width 200  # 仅 pprint 模式下行宽
  python3 inspect_pkl.py path/to/file.pkl --raw-one   # 只读文件中第一个 pickle 对象（未做连续 load 合并）

注意: 不要对不可信来源的 pkl 执行 unpickle（存在执行任意代码风险）。
"""
from __future__ import print_function

import argparse
import json
import os
import pickle
import pprint
import sys


def _terminal_width(default=200):
    try:
        return max(120, min(os.get_terminal_size().columns - 2, 300))
    except Exception:
        return default


def _to_jsonable(obj, depth=0):
    """将 dict/list/tuple 等转为可 json.dumps 的结构（仅用于展示）。"""
    if depth > 80:
        return "..."
    if obj is None or isinstance(obj, (bool, int, float)):
        return obj
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            out[str(k)] = _to_jsonable(v, depth + 1)
        return out
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(x, depth + 1) for x in obj]
    if isinstance(obj, (bytes, bytearray)):
        try:
            return obj.decode("utf-8", errors="replace")
        except Exception:
            return str(obj)
    return str(obj)


def _format_preview(obj, use_json, pp):
    if use_json:
        try:
            text = json.dumps(_to_jsonable(obj), indent=2, ensure_ascii=False)
            print(text)
        except (TypeError, ValueError) as e:
            print("(JSON 预览失败: {}, 回退 pprint)".format(e))
            pp.pprint(obj)
    else:
        pp.pprint(obj)


def load_all_objects(path):
    """与 test.py 一致：同一文件内可能有多个连续 dump，逐个 load 直到 EOF。"""
    objects = []
    with open(path, "rb") as f:
        while True:
            try:
                objects.append(pickle.load(f))
            except EOFError:
                break
    return objects


def summarize(obj, head, pp, use_json=False):
    t = type(obj).__name__
    print("类型: {}".format(t))

    if isinstance(obj, list):
        print("长度: {}".format(len(obj)))
        if not obj:
            return
        print("首元素类型: {}".format(type(obj[0]).__name__))
        sample = obj[:head]
        print("前 {} 条预览:".format(len(sample)))
        _format_preview(sample, use_json, pp)

    elif isinstance(obj, dict):
        keys = list(obj.keys())
        print("键数量: {}".format(len(keys)))
        show_keys = keys[: min(30, len(keys))]
        print("键预览 (至多 30 个): {}".format(show_keys))
        for k in show_keys[: min(head, len(show_keys))]:
            v = obj[k]
            print("\n--- key: {!r} ---".format(k))
            if isinstance(v, list):
                print("  value 类型: list, 长度: {}".format(len(v)))
                _format_preview(v[:head], use_json, pp)
            else:
                _format_preview(v, use_json, pp)

    else:
        print("内容预览:")
        _format_preview(obj, use_json, pp)


def main():
    parser = argparse.ArgumentParser(description="查看 pickle (.pkl) 文件结构与前几条内容")
    parser.add_argument("path", help=".pkl 文件路径")
    parser.add_argument(
        "--head",
        type=int,
        default=3,
        help="列表/字典抽样时最多展示几条（默认 3）",
    )
    parser.add_argument(
        "--raw-one",
        action="store_true",
        help="只读取并展示文件中的第一个 pickle 对象（不尝试读多个连续对象）",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="用缩进 JSON 预览（tuple 会显示为数组），嵌套结构更易读",
    )
    parser.add_argument(
        "--width",
        type=int,
        default=None,
        help="pprint 行宽（默认：尽量贴合终端宽度，约 120–300；仅非 --json 时有效）",
    )
    args = parser.parse_args()

    path = args.path
    pw = args.width if args.width is not None else _terminal_width()
    pp = pprint.PrettyPrinter(indent=2, width=pw, compact=False)

    try:
        if args.raw_one:
            with open(path, "rb") as f:
                obj = pickle.load(f)
            print("=== 单个 pickle 对象 ===\n")
            summarize(obj, args.head, pp, use_json=args.json)
        else:
            objs = load_all_objects(path)
            print("文件中 pickle 对象个数: {}\n".format(len(objs)))
            for i, obj in enumerate(objs):
                print("========== 对象 #{} ==========".format(i))
                summarize(obj, args.head, pp, use_json=args.json)
                print()
    except Exception as e:
        print("读取失败: {}".format(e), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
