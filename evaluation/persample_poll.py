# Poll a running job's per-vertex record counters every 2 s (JobManager REST on :18081) until the source
# has written <target> records. Usage: python persample_poll.py <jobId> <target> <out.json>
# Used for the Exp C per-sample run (data/persample_500hz.csv); submit the job first with `flink run -d`.
import json, sys, time, urllib.request
jid, target, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]; t0 = time.time(); rows = []
def get(p):
    with urllib.request.urlopen("http://localhost:18081" + p, timeout=10) as r: return json.loads(r.read())
while time.time() - t0 < 600:
    j = get(f"/jobs/{jid}"); t = round(time.time(), 2)
    vs = {v["name"][:12]: (v["metrics"].get("read-records"), v["metrics"].get("write-records")) for v in j["vertices"]}
    src_w = vs["Source: kafk"][1]; s2_r = vs["stage2-agg -"][0]; s2_w = vs["stage2-agg -"][1]
    rows.append((t, src_w, s2_r, s2_w)); print(f"{time.strftime('%H:%M:%S')} src_out={src_w:>9} s2_in={s2_r:>9} s2_out={s2_w:>6}", flush=True)
    if src_w >= target: print("TARGET", flush=True); break
    time.sleep(2)
json.dump(rows, open(out, "w")); print("DONE", flush=True)
