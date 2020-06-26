import pandas as pd
import os


def _saexp1(data_dir):
    file = "SAEXP1_1997_2017_ALL_AREAS_.csv"
    t = pd.read_csv(
        os.path.join(data_dir, file),
        index_col=None,
        engine="c",
        nrows=1440,
        low_memory=False,
    )

    t["GeoFIPS"] = t["GeoFIPS"].replace({'"': ""}, regex=True)
    t["GeoFIPS"] = t["GeoFIPS"].map(int)

    # melt data
    t = pd.melt(t, id_vars=t.keys()[0:9], var_name="year")

    # typing
    t["GeoFIPS"] = t["GeoFIPS"].map(str)
    t["GeoName"] = t["GeoName"].map(str)
    t["Region"] = t["Region"].map(str)
    t["TableName"] = t["TableName"].map(str)
    t["ComponentName"] = t["ComponentName"].map(str)
    t["Unit"] = t["Unit"].map(str)
    t["Line"] = t["Line"].map(str)
    t["IndustryClassification"] = t["IndustryClassification"].map(str)
    t["Description"] = t["Description"].map(str)
    t["year"] = t["year"].map(str)
    t["value"] = t["value"].map(float)

    return t


def _saexp2(data_dir):
    file = "SAEXP2_1997_2017_ALL_AREAS_.csv"
    t = pd.read_csv(
        os.path.join(data_dir, file),
        index_col=None,
        engine="c",
        nrows=1440,
        low_memory=False,
    )

    t["GeoFIPS"] = t["GeoFIPS"].replace({'"': ""}, regex=True)
    t["GeoFIPS"] = t["GeoFIPS"].map(int)

    # melt data
    t = pd.melt(t, id_vars=t.keys()[0:9], var_name="year")

    # typing
    t["GeoFIPS"] = t["GeoFIPS"].map(str)
    t["GeoName"] = t["GeoName"].map(str)
    t["Region"] = t["Region"].map(str)
    t["TableName"] = t["TableName"].map(str)
    t["ComponentName"] = t["ComponentName"].map(str)
    t["Unit"] = t["Unit"].map(str)
    t["Line"] = t["Line"].map(str)
    t["IndustryClassification"] = t["IndustryClassification"].map(str)
    t["Description"] = t["Description"].map(str)
    t["year"] = t["year"].map(str)
    t["value"] = t["value"].map(float)

    return t
