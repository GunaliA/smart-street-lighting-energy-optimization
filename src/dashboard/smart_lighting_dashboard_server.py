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
    q = parse_qs(urlparse(path).query)

    return {
        "location": q.get("location", ["All"])[0],
        "traffic": q.get("traffic", ["All"])[0],
        "light_status": q.get("light_status", ["All"])[0],
        "fault_status": q.get("fault_status", ["All"])[0],
    }


def filtered(p):
    d = DF

    if p["location"] != "All":
        d = d[d.location == p["location"]]

    if p["traffic"] != "All":
        d = d[d.traffic_level == p["traffic"]]

    if p["light_status"] != "All":
        d = d[d.light_status == p["light_status"]]

    if p["fault_status"] != "All":
        d = d[d.fault_status == p["fault_status"]]

    return d


def add_energy(d):
    x = d.copy()

    x["energy_kwh"] = (
        x["power_consumption"]
        * SAMPLE_INTERVAL_SECONDS
        / 3_600_000.0
    )

    return x


def latest_per_light(d):
    if d.empty:
        return d

    return (
        d.sort_values("timestamp")
        .drop_duplicates("light_id", keep="last")
    )


def result(endpoint, p):
    d = add_energy(filtered(p))

    # ---------------------------------------------------------
    # Current API endpoints
    # ---------------------------------------------------------

    if endpoint == "kpis":
        latest = latest_per_light(d)

        return pd.DataFrame({
            "total_lights": [d.light_id.nunique()],
            "total_energy_kwh": [d.energy_kwh.sum()],
            "average_power_w": [
                d.power_consumption.mean() if len(d) else 0
            ],
            "active_lights": [
                latest.loc[
                    latest.light_status == "ON",
                    "light_id"
                ].nunique()
            ],
            "faulty_lights": [
                latest.loc[
                    latest.fault_status != "NORMAL",
                    "light_id"
                ].nunique()
            ],
        })

    if endpoint == "location_energy":
        return (
            d.groupby("location", as_index=False)
            .agg(
                record_count=("light_id", "size"),
                total_energy_kwh=("energy_kwh", "sum"),
                average_power_w=("power_consumption", "mean"),
                unique_lights=("light_id", "nunique"),
            )
        )

    if endpoint == "traffic":
        return (
            d.groupby("traffic_level", as_index=False)
            .agg(
                record_count=("traffic_level", "size"),
                total_energy_kwh=("energy_kwh", "sum"),
                average_power_w=("power_consumption", "mean"),
            )
        )

    if endpoint == "motion":
        d["motion_status"] = d.motion_detected.map({
            1: "Motion Detected",
            0: "No Motion",
        })

        return (
            d.groupby("motion_status", as_index=False)
            .agg(
                record_count=("light_id", "size"),
                total_energy_kwh=("energy_kwh", "sum"),
                average_power_w=("power_consumption", "mean"),
            )
        )

    if endpoint == "brightness":
        return (
            d.groupby("brightness_level", as_index=False)
            .agg(
                record_count=("brightness_level", "size"),
                average_power_w=("power_consumption", "mean"),
            )
        )

    if endpoint == "temperature":
        bins = [
            float("-inf"),
            25,
            30,
            35,
            float("inf"),
        ]

        labels = [
            "Below 25°C",
            "25°C - 29.9°C",
            "30°C - 34.9°C",
            "35°C and above",
        ]

        d["temperature_range"] = pd.cut(
            d.temperature,
            bins=bins,
            labels=labels,
            right=False,
        )

        return (
            d.groupby(
                "temperature_range",
                observed=False,
                as_index=False,
            )
            .agg(
                record_count=("temperature", "size"),
                average_temperature=("temperature", "mean"),
                average_power_w=("power_consumption", "mean"),
            )
        )

    if endpoint == "power_time":
        d["hour"] = d.timestamp.dt.floor("h")

        return (
            d.groupby("hour", as_index=False)
            .agg(
                record_count=("light_id", "size"),
                total_energy_kwh=("energy_kwh", "sum"),
                average_power_w=("power_consumption", "mean"),
            )
        )

    if endpoint == "light_status":
        return (
            d.groupby("light_status", as_index=False)
            .size()
            .rename(columns={"size": "record_count"})
        )

    if endpoint == "fault_status":
        return (
            d.groupby("fault_status", as_index=False)
            .size()
            .rename(columns={"size": "record_count"})
        )

    if endpoint == "recent":
        return (
            d.sort_values("timestamp", ascending=False)
            .head(20)
            .assign(
                energy_kwh=lambda x: x.energy_kwh.round(8)
            )
        )

    if endpoint == "filtered":
        return d.head(1000)

    if endpoint == "filters":
        return pd.DataFrame({
            "locations": [
                json.dumps(sorted(DF.location.unique().tolist()))
            ],
            "traffic": [
                json.dumps(sorted(DF.traffic_level.unique().tolist()))
            ],
            "light_status": [
                json.dumps(sorted(DF.light_status.unique().tolist()))
            ],
            "fault_status": [
                json.dumps(sorted(DF.fault_status.unique().tolist()))
            ],
        })

    return pd.DataFrame({
        "error": ["unknown endpoint"]
    })


# -------------------------------------------------------------
# Compatibility layer for the existing Grafana v3 dashboard
# -------------------------------------------------------------

def legacy_result(filename, p):
    name = filename.removesuffix(".csv")

    # KPI panels
    if name == "kpi_total_lights":
        d = result("kpis", p)
        return pd.DataFrame({
            "value": [d.iloc[0]["total_lights"]]
        })

    if name == "kpi_active_lights":
        d = result("kpis", p)
        return pd.DataFrame({
            "value": [d.iloc[0]["active_lights"]]
        })

    if name == "kpi_faulty_lights":
        d = result("kpis", p)
        return pd.DataFrame({
            "value": [d.iloc[0]["faulty_lights"]]
        })

    if name == "kpi_total_energy":
        d = result("kpis", p)
        return pd.DataFrame({
            "value": [d.iloc[0]["total_energy_kwh"]]
        })

    if name == "kpi_average_power":
        d = result("kpis", p)
        return pd.DataFrame({
            "value": [d.iloc[0]["average_power_w"]]
        })

    # Power over time
    if name == "power_over_time":
        d = result("power_time", p)

        d = d.rename(columns={
            "hour": "timestamp",
            "total_energy_kwh": "total_energy",
            "average_power_w": "average_power",
        })

        d["timestamp"] = d["timestamp"].dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        return d

    # Location energy
    if name == "location_energy":
        d = result("location_energy", p)

        return d.rename(columns={
            "total_energy_kwh": "total_energy",
            "average_power_w": "average_power",
        })

    # Traffic analysis
    if name == "traffic_analysis":
        d = result("traffic", p)

        return d.rename(columns={
            "traffic_level": "traffic",
            "total_energy_kwh": "total_energy",
            "average_power_w": "average_power",
        })

    # Traffic distribution
    if name == "traffic_distribution":
        d = result("traffic", p)

        return d.rename(columns={
            "traffic_level": "traffic"
        })[[
            "traffic",
            "record_count",
        ]]

    # Motion analysis
    if name == "motion_power":
        d = result("motion", p)

        return d.rename(columns={
            "average_power_w": "average_power",
            "total_energy_kwh": "total_energy",
        })

    # Light status
    if name == "light_status":
        return result("light_status", p)

    # Fault status
    if name == "fault_status":
        return result("fault_status", p)

    # Brightness analysis
    if name == "brightness_analysis":
        return result("brightness", p)

    # Temperature analysis
    if name == "temperature_analysis":
        return result("temperature", p)

    # Recent records
    if name == "recent_records":
        return result("recent", p)

    return pd.DataFrame({
        "error": [f"unknown dashboard file: {filename}"]
    })


def csv_bytes(df):
    out = io.StringIO()
    df.to_csv(out, index=False)
    return out.getvalue().encode("utf-8")


class Handler(BaseHTTPRequestHandler):

    def do_GET(self):
        parsed = urlparse(self.path)
        parts = parsed.path.strip("/").split("/")

        try:
            # -------------------------------------------------
            # Current API
            # -------------------------------------------------
            if len(parts) >= 2 and parts[0] == "api":
                endpoint = parts[1]

                data = csv_bytes(
                    result(endpoint, params(self.path))
                )

                self.send_response(200)
                self.send_header(
                    "Content-Type",
                    "text/csv; charset=utf-8"
                )
                self.send_header(
                    "Access-Control-Allow-Origin",
                    "*"
                )
                self.send_header(
                    "Content-Length",
                    str(len(data))
                )
                self.end_headers()
                self.wfile.write(data)
                return

            # -------------------------------------------------
            # Legacy Grafana dashboard URLs
            # -------------------------------------------------
            if len(parts) >= 2 and parts[0] == "dashboard":
                filename = parts[1]

                data = csv_bytes(
                    legacy_result(
                        filename,
                        params(self.path)
                    )
                )

                self.send_response(200)
                self.send_header(
                    "Content-Type",
                    "text/csv; charset=utf-8"
                )
                self.send_header(
                    "Access-Control-Allow-Origin",
                    "*"
                )
                self.send_header(
                    "Content-Length",
                    str(len(data))
                )
                self.end_headers()
                self.wfile.write(data)
                return

            self.send_response(404)
            self.end_headers()

        except Exception as e:
            data = str(e).encode("utf-8")

            self.send_response(500)
            self.send_header(
                "Content-Type",
                "text/plain; charset=utf-8"
            )
            self.send_header(
                "Access-Control-Allow-Origin",
                "*"
            )
            self.send_header(
                "Content-Length",
                str(len(data))
            )
            self.end_headers()
            self.wfile.write(data)

    def log_message(self, fmt, *args):
        print(fmt % args)


print(f"Loaded {len(DF):,} records from {DATA}")
print(f"Dashboard API: http://{HOST}:{PORT}/api/kpis")
print("Grafana legacy CSV endpoints enabled.")

ThreadingHTTPServer(
    (HOST, PORT),
    Handler
).serve_forever()