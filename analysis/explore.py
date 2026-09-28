import pandas as pd, numpy as np, os
BASE="data"
sets=["normal-thingo-1user-5min","abnormal-thingo-1user-5min","abnormal-thingo-1user-10min",
      "abnormal-thingo-10user-10min","abnormal-thingo-10user-10min-summarizer"]
def m(s,f): return os.path.join(BASE,s,"agent-network","metrics",f)
for s in sets:
    print("="*70); print(s)
    inst=pd.read_csv(m(s,"instance.csv"))
    inst['timestamp']=pd.to_datetime(inst['timestamp'])
    dur=(inst['timestamp'].max()-inst['timestamp'].min())
    print(f"  span: {inst['timestamp'].min()} -> {inst['timestamp'].max()}  ({dur}), rows={len(inst)}")
    # sampling interval
    dt=inst['timestamp'].diff().dropna().dt.total_seconds()
    print(f"  sample interval sec: median={dt.median():.0f}")
    # instances_num scaling
    innum=pd.read_csv(m(s,"instances_num.csv"))
    tcol=[c for c in innum.columns if 'time' in c.lower() or innum[c].astype(str).str.contains('2026').any()]
    cntcols=[c for c in innum.columns if c.endswith('count')]
    scale={}
    for c in cntcols:
        v=pd.to_numeric(innum[c],errors='coerce')
        if v.nunique()>1: scale[c.replace('&count','')]=(int(v.min()),int(v.max()))
    print(f"  scaling services (min,max instances): {scale}")
    # call sparsity
    call=pd.read_csv(m(s,"call.csv"))
    datacols=[c for c in call.columns if c!='timestamp']
    total=call[datacols].size
    empty=call[datacols].isna().sum().sum()
    print(f"  call.csv sparsity: {empty}/{total} = {empty/total:.1%} empty")
    # svc cpu variance across services
    svc=pd.read_csv(m(s,"svc_metric.csv"))
    cpucols=[c for c in svc.columns if c.endswith('&cpu_usage')]
    means=svc[cpucols].mean()
    print(f"  svc cpu_usage mean range: {means.min():.4f} .. {means.max():.4f}  (max/min ratio={means.max()/max(means.min(),1e-6):.0f})")
