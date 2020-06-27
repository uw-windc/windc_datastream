import pandas as pd
import os


def _2012(data_dir):
    file = "cfs_2012_pumf_csv.txt"

    a = pd.read_csv(
        os.path.join(data_dir, file), index_col=None, low_memory=False, engine="c"
    )

    # # adding HAZMAT description from supplemental data files
    # hazmat = {
    #     "P": "Class 3 HAZMAT (flammable liquids)",
    #     "H": "Other HAZMAT",
    #     "N": "Not HAZMAT",
    # }
    # a["HAZMAT"] = a["HAZMAT"].map(hazmat)
    #
    # # Export country
    # cntry = {"C": "Canada", "M": "Mexico", "O": "Other"}
    # a["EXPORT_CNTRY"] = a["EXPORT_CNTRY"].map(cntry)

    # adding in units
    a["SHIPMT_VALUE_units"] = "us dollars (USD)"
    a["SHIPMT_WGHT_units"] = "weight of shipment in pounds"
    a["SHIPMT_DIST_GC_units"] = "great circle distance in miles"
    a["SHIPMT_DIST_ROUTED_units"] = "routed distance in miles"

    a["year"] = "2012"

    a["SHIPMT_ID"] = a["SHIPMT_ID"].map(str)
    a["ORIG_STATE"] = a["ORIG_STATE"].map(str)
    a["ORIG_MA"] = a["ORIG_MA"].map(str)
    a["ORIG_CFS_AREA"] = a["ORIG_CFS_AREA"].map(str)
    a["DEST_STATE"] = a["DEST_STATE"].map(str)
    a["DEST_MA"] = a["DEST_MA"].map(str)
    a["DEST_CFS_AREA"] = a["DEST_CFS_AREA"].map(str)
    a["NAICS"] = a["NAICS"].map(str)
    a["QUARTER"] = a["QUARTER"].map(str)
    a["SCTG"] = a["SCTG"].map(str)
    a["MODE"] = a["MODE"].map(str)
    a["SHIPMT_VALUE"] = a["SHIPMT_VALUE"].map(float)
    a["SHIPMT_WGHT"] = a["SHIPMT_WGHT"].map(float)
    a["SHIPMT_DIST_GC"] = a["SHIPMT_DIST_GC"].map(float)
    a["SHIPMT_DIST_ROUTED"] = a["SHIPMT_DIST_ROUTED"].map(float)
    a["TEMP_CNTL_YN"] = a["TEMP_CNTL_YN"].map(str)
    a["EXPORT_YN"] = a["EXPORT_YN"].map(str)
    a["EXPORT_CNTRY"] = a["EXPORT_CNTRY"].map(str)
    a["HAZMAT"] = a["HAZMAT"].map(str)
    a["WGT_FACTOR"] = a["WGT_FACTOR"].map(float)
    a["year"] = a["year"].map(str)
    a["SHIPMT_VALUE_units"] = a["SHIPMT_VALUE_units"].map(str)
    a["SHIPMT_WGHT_units"] = a["SHIPMT_WGHT_units"].map(str)
    a["SHIPMT_DIST_GC_units"] = a["SHIPMT_DIST_GC_units"].map(str)
    a["SHIPMT_DIST_ROUTED_units"] = a["SHIPMT_DIST_ROUTED_units"].map(str)

    # read in appendix file
    sup_file = "cfs_2012_pum_file_users_guide_App_A (Jun 2015).xlsx"
    apdx_1 = pd.read_excel(
        os.path.join(data_dir, sup_file), skiprows=1, sheet_name="App A1"
    )

    # appendix 1
    apdx_1.drop([0], inplace=True)
    apdx_1.rename(
        columns={
            "ORIG_MA": "CFS_MA",
            "ORIG_STATE": "CFS_STATE",
            "ORIG_CFS_AREA": "CFS_AREA",
        },
        inplace=True,
    )

    apdx_1["CFS_MA"] = apdx_1["CFS_MA"].map(str)
    apdx_1["CFS_STATE"] = apdx_1["CFS_STATE"].map(str)
    apdx_1["CFS_AREA"] = apdx_1["CFS_AREA"].map(str)
    apdx_1["MA"] = apdx_1["MA"].map(str)
    apdx_1["Description"] = apdx_1["Description"].map(str)

    # appendix 2
    apdx_2 = pd.read_excel(
        os.path.join(data_dir, sup_file), skiprows=1, sheet_name="App A2"
    )

    apdx_2["NAICS"] = apdx_2["NAICS"].map(str)
    apdx_2["Description"] = apdx_2["Description"].map(str)

    # appendix 3
    apdx_3 = pd.read_excel(
        os.path.join(data_dir, sup_file), skiprows=1, sheet_name="App A3"
    )
    apdx_3["SCTG Group"] = apdx_3["SCTG Group"].interpolate(method="pad")

    apdx_3["SCTG"] = apdx_3["SCTG"].map(str)
    apdx_3["Description"] = apdx_3["Description"].map(str)
    apdx_3["SCTG Group"] = apdx_3["SCTG Group"].map(str)

    # appendix 4
    apdx_4 = pd.read_excel(
        os.path.join(data_dir, sup_file),
        skiprows=1,
        usecols="A:B",
        nrows=21,
        sheet_name="App A4",
    )

    apdx_4.rename(
        columns={"Mode of transportation Description": "Description"}, inplace=True,
    )

    apdx_4["Mode Code"] = apdx_4["Mode Code"].map(str)
    apdx_4["Description"] = apdx_4["Description"].map(str)

    return a, apdx_1, apdx_2, apdx_3, apdx_4
