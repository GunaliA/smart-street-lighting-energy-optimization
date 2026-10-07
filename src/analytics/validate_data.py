import pandas as pd
DATA_FILE="data/sample/street_light_data.csv"
EXPECTED_COLUMNS=["timestamp","light_id","location","latitude","longitude","ldr_value","motion_detected","traffic_level","brightness_level","voltage","current","power_consumption","temperature","light_status","fault_status"]

def validate_dataset():
    df=pd.read_csv(DATA_FILE)
    errors=[]
    if len(df)!=100_000: errors.append(f"Expected 100,000 records; found {len(df):,}")
    if df.light_id.nunique()!=10_000: errors.append(f"Expected 10,000 unique lights; found {df.light_id.nunique():,}")
    if list(df.columns)!=EXPECTED_COLUMNS: errors.append("Column layout does not match expected schema")
    if df.isnull().sum().sum(): errors.append("Missing values found")
    if df.duplicated().sum(): errors.append("Duplicate rows found")
    locations_per_light=df.groupby("light_id").location.nunique()
    if (locations_per_light>1).any(): errors.append("A light_id belongs to multiple zones")
    coords=df.groupby("light_id")[["latitude","longitude"]].nunique().max()
    if (coords>1).any(): errors.append("A light_id has changing coordinates")
    zone_counts=df.groupby("location").light_id.nunique()
    if set(zone_counts.tolist())!={1000}: errors.append(f"Zone inventory is not 1,000 lights per zone: {zone_counts.to_dict()}")
    if not set(df.traffic_level.unique()) <= {"LOW","MEDIUM","HIGH"}: errors.append("Invalid traffic level")
    if not set(df.fault_status.unique()) <= {"NORMAL","LOW_VOLTAGE","HIGH_CURRENT","LIGHT_FAILURE"}: errors.append("Invalid fault status")
    print("SMART STREET LIGHTING DATASET VALIDATION")
    print(f"Records: {len(df):,} | Unique lights: {df.light_id.nunique():,} | Missing: {int(df.isnull().sum().sum())} | Duplicates: {int(df.duplicated().sum())}")
    print("Lights per zone:", zone_counts.to_dict())
    if errors:
        print("VALIDATION FAILED")
        for e in errors: print(" -",e)
        raise SystemExit(1)
    print("VALIDATION PASSED")

if __name__=="__main__": validate_dataset()
