from pathlib import Path
import pandas as pd

PROJECT=Path(__file__).resolve().parents[2]
SAMPLE=PROJECT/"data/sample/street_light_data.csv"
OUT=PROJECT/"data/dashboard"
OUT.mkdir(parents=True,exist_ok=True)
INTERVAL=1

df=pd.read_csv(SAMPLE,parse_dates=["timestamp"])
df["energy_kwh"]=df.power_consumption*INTERVAL/3_600_000.0

latest=df.sort_values("timestamp").drop_duplicates("light_id",keep="last")
kpis=pd.DataFrame({"metric":["Total Lights","Total Energy (kWh)","Average Power (W)","Active Lights (latest)","Faulty Lights (latest)"],"value":[df.light_id.nunique(),df.energy_kwh.sum(),df.power_consumption.mean(),latest.loc[latest.light_status=="ON","light_id"].nunique(),latest.loc[latest.fault_status!="NORMAL","light_id"].nunique()]})
kpis.to_csv(OUT/"kpis.csv",index=False)

df.groupby("location",as_index=False).agg(record_count=("light_id","size"),unique_lights=("light_id","nunique"),total_energy_kwh=("energy_kwh","sum"),average_power_w=("power_consumption","mean")).to_csv(OUT/"location_energy.csv",index=False)
df.groupby("traffic_level",as_index=False).agg(record_count=("traffic_level","size"),total_energy_kwh=("energy_kwh","sum"),average_power_w=("power_consumption","mean")).to_csv(OUT/"traffic_analysis.csv",index=False)
df.traffic_level.value_counts().rename_axis("traffic_level").reset_index(name="record_count").to_csv(OUT/"traffic_distribution.csv",index=False)
df.assign(motion_status=df.motion_detected.map({1:"Motion Detected",0:"No Motion"})).groupby("motion_status",as_index=False).agg(record_count=("light_id","size"),total_energy_kwh=("energy_kwh","sum"),average_power_w=("power_consumption","mean")).to_csv(OUT/"motion_power.csv",index=False)
df.groupby("brightness_level",as_index=False).agg(record_count=("brightness_level","size"),average_power_w=("power_consumption","mean")).to_csv(OUT/"brightness_analysis.csv",index=False)
bins=[float("-inf"),25,30,35,float("inf")]; labels=["Below 25°C","25°C - 29.9°C","30°C - 34.9°C","35°C and above"]
t=df.copy(); t["temperature_range"]=pd.cut(t.temperature,bins=bins,labels=labels,right=False); t.groupby("temperature_range",observed=False,as_index=False).agg(record_count=("temperature","size"),average_temperature=("temperature","mean"),average_power_w=("power_consumption","mean")).to_csv(OUT/"temperature_analysis.csv",index=False)
p=df.copy(); p["time_period"]=p.timestamp.dt.floor("h"); p.groupby("time_period",as_index=False).agg(record_count=("light_id","size"),total_energy_kwh=("energy_kwh","sum"),average_power_w=("power_consumption","mean")).to_csv(OUT/"power_over_time.csv",index=False)
df.groupby("light_status",as_index=False).size().rename(columns={"size":"record_count"}).to_csv(OUT/"light_status.csv",index=False)
df.groupby("fault_status",as_index=False).size().rename(columns={"size":"record_count"}).to_csv(OUT/"fault_status.csv",index=False)
df.sort_values("timestamp",ascending=False).head(20).to_csv(OUT/"recent_records.csv",index=False)
df.to_csv(OUT/"filtered_dataset.csv",index=False)
print(f"Prepared dashboard data in {OUT}")
print(kpis.to_string(index=False))
