from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from pathlib import Path
import io
import json
import pandas as pd

PROJECT = Path(__file__).resolve().parents[2]
DATA = PROJECT / "data" / "sample" / "street_light_data.csv"
HOST = "127.0.0.1"
PORT = 8000
SAMPLE_INTERVAL_SECONDS = 1

DF = pd.read_csv(DATA, parse_dates=["timestamp"])

def params(path):
    q=parse_qs(urlparse(path).query)
    return {k:q.get(k,["All"])[0] for k in ("location","traffic","light_status","fault_status")}

def filtered(p):
    d=DF
    if p["location"]!="All": d=d[d.location==p["location"]]
    if p["traffic"]!="All": d=d[d.traffic_level==p["traffic"]]
    if p["light_status"]!="All": d=d[d.light_status==p["light_status"]]
    if p["fault_status"]!="All": d=d[d.fault_status==p["fault_status"]]
    return d

def add_energy(d):
    x=d.copy(); x["energy_kwh"]=x["power_consumption"]*SAMPLE_INTERVAL_SECONDS/3_600_000.0; return x

def latest_per_light(d):
    if d.empty: return d
    return d.sort_values("timestamp").drop_duplicates("light_id",keep="last")

def result(endpoint,p):
    d=add_energy(filtered(p))
    if endpoint=="kpis":
        latest=latest_per_light(d); return pd.DataFrame({"total_lights":[d.light_id.nunique()],"total_energy_kwh":[d.energy_kwh.sum()],"average_power_w":[d.power_consumption.mean() if len(d) else 0],"active_lights":[latest.loc[latest.light_status=="ON","light_id"].nunique()],"faulty_lights":[latest.loc[latest.fault_status!="NORMAL","light_id"].nunique()]})
    if endpoint=="location_energy": return d.groupby("location",as_index=False).agg(record_count=("light_id","size"),total_energy_kwh=("energy_kwh","sum"),average_power_w=("power_consumption","mean"),unique_lights=("light_id","nunique"))
    if endpoint=="traffic": return d.groupby("traffic_level",as_index=False).agg(record_count=("traffic_level","size"),total_energy_kwh=("energy_kwh","sum"),average_power_w=("power_consumption","mean"))
    if endpoint=="motion":
        d["motion_status"]=d.motion_detected.map({1:"Motion Detected",0:"No Motion"}); return d.groupby("motion_status",as_index=False).agg(record_count=("light_id","size"),total_energy_kwh=("energy_kwh","sum"),average_power_w=("power_consumption","mean"))
    if endpoint=="brightness": return d.groupby("brightness_level",as_index=False).agg(record_count=("brightness_level","size"),average_power_w=("power_consumption","mean"))
    if endpoint=="temperature":
        bins=[float("-inf"),25,30,35,float("inf")]; labels=["Below 25°C","25°C - 29.9°C","30°C - 34.9°C","35°C and above"]; d["temperature_range"]=pd.cut(d.temperature,bins=bins,labels=labels,right=False); return d.groupby("temperature_range",observed=False,as_index=False).agg(record_count=("temperature","size"),average_temperature=("temperature","mean"),average_power_w=("power_consumption","mean"))
    if endpoint=="power_time":
        d["hour"]=d.timestamp.dt.floor("h"); x=d.groupby("hour",as_index=False).agg(record_count=("light_id","size"),total_energy_kwh=("energy_kwh","sum"),average_power_w=("power_consumption","mean")); x["hour"]=x.hour.dt.strftime("%Y-%m-%d %H:%M:%S"); return x
    if endpoint=="light_status": return d.groupby("light_status",as_index=False).size().rename(columns={"size":"record_count"})
    if endpoint=="fault_status": return d.groupby("fault_status",as_index=False).size().rename(columns={"size":"record_count"})
    if endpoint=="recent": return d.sort_values("timestamp",ascending=False).head(20).assign(energy_kwh=lambda x:x.energy_kwh.round(8))
    if endpoint=="filtered": return d.head(1000)
    if endpoint=="filters": return pd.DataFrame({"locations": [json.dumps(sorted(DF.location.unique().tolist()))],"traffic": [json.dumps(sorted(DF.traffic_level.unique().tolist()))],"light_status": [json.dumps(sorted(DF.light_status.unique().tolist()))],"fault_status": [json.dumps(sorted(DF.fault_status.unique().tolist()))]})
    return pd.DataFrame({"error":["unknown endpoint"]})

def csv_bytes(df):
    out=io.StringIO(); df.to_csv(out,index=False); return out.getvalue().encode("utf-8")

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed=urlparse(self.path); parts=parsed.path.strip("/").split("/")
        if len(parts)>=2 and parts[0]=="api":
            try:
                data=csv_bytes(result(parts[1],params(self.path))); self.send_response(200); self.send_header("Content-Type","text/csv; charset=utf-8"); self.send_header("Access-Control-Allow-Origin","*"); self.send_header("Content-Length",str(len(data))); self.end_headers(); self.wfile.write(data)
            except Exception as e:
                data=str(e).encode(); self.send_response(500); self.send_header("Content-Type","text/plain"); self.end_headers(); self.wfile.write(data)
            return
        self.send_response(404); self.end_headers()
    def log_message(self,fmt,*args): print(fmt%args)

print(f"Loaded {len(DF):,} records from {DATA}")
print(f"Dashboard API: http://{HOST}:{PORT}/api/kpis")
ThreadingHTTPServer((HOST,PORT),Handler).serve_forever()
