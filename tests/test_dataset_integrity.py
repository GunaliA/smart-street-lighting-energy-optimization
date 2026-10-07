import pandas as pd

def test_dataset_integrity():
    df=pd.read_csv("data/sample/street_light_data.csv")
    assert len(df)==100_000
    assert df.light_id.nunique()==10_000
    assert df.groupby("location").light_id.nunique().eq(1000).all()
    assert df.groupby("light_id").location.nunique().eq(1).all()
    assert df.groupby("light_id")[['latitude','longitude']].nunique().max().max()==1
    assert df.isnull().sum().sum()==0
    assert df.duplicated().sum()==0
