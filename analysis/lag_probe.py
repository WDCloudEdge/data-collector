import pandas as pd, numpy as np, os
BASE="data"
def m(s,f): return os.path.join(BASE,s,"agent-network","metrics",f)
for s in ["abnormal-thingo-10user-10min-summarizer","abnormal-thingo-10user-10min","abnormal-thingo-1user-10min"]:
    print("#"*80); print(s)
    lat=pd.read_csv(m(s,"latency.csv")); lat['timestamp']=pd.to_datetime(lat['timestamp'])
    inst=pd.read_csv(m(s,"instance.csv")); inst['timestamp']=pd.to_datetime(inst['timestamp'])
    innum=pd.read_csv(m(s,"instances_num.csv"))
    # planner & summarizer latency p50/p90
    for svc in ['planner','summarizer']:
        cols=[c for c in lat.columns if svc in c]
        print(f"  {svc} latency cols: {cols}")
    # build per-15s downsample to per-30s to read
    t0=lat['timestamp'].min()
    lat['t']=((lat['timestamp']-t0).dt.total_seconds()).astype(int)
    inst['t']=((inst['timestamp']-inst['timestamp'].min()).dt.total_seconds()).astype(int)
    pl=[c for c in lat.columns if 'planner&p90' in c]
    su=[c for c in lat.columns if 'summarizer&p90' in c]
    plcpu=[c for c in inst.columns if 'planner' in c and c.endswith('_cpu')]
    sucpu=[c for c in inst.columns if 'summarizer' in c and c.endswith('_cpu')]
    print(f"  {'t(s)':>5} {'plan_lat_p90':>12} {'sum_lat_p90':>12} {'plan_cpu':>9} {'sum_cpu':>9}")
    for i in range(0,len(lat),12):  # every 60s
        r=lat.iloc[i]; ri=inst.iloc[min(i,len(inst)-1)]
        pv=r[pl[0]] if pl else np.nan
        sv=r[su[0]] if su else np.nan
        pc=ri[plcpu[0]] if plcpu else np.nan
        sc=ri[sucpu[0]] if sucpu else np.nan
        print(f"  {int(r['t']):>5} {pv:>12.0f} {sv:>12.0f} {pc:>9.2f} {sc:>9.2f}")
