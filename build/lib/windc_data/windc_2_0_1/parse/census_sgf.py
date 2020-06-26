import pandas as pd
import os


def _1996(data_dir):
    from . import sgf_table_sums

    file = "96data35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_1992_2004.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_1992_2004[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "1996"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _1997(data_dir):
    from . import sgf_table_sums

    file = "97data35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_1992_2004.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_1992_2004[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "1997"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _1998(data_dir):
    from . import sgf_table_sums

    file = "98data35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_1992_2004.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_1992_2004[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "1998"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _1999(data_dir):
    from . import sgf_table_sums

    file = "99state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Origin"] = [t.loc[i, 0][17:19] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][21:24] for i in t.index]
    t["Amount"] = [t.loc[i, 0][24:35] for i in t.index]
    t["Survery Year"] = 99
    t["Year of Data"] = 99

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_1992_2004.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_1992_2004[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "1999"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2000(data_dir):
    from . import sgf_table_sums

    file = "00state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_1992_2004.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_1992_2004[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2000"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2001(data_dir):
    from . import sgf_table_sums

    file = "01state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_1992_2004.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_1992_2004[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2001"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2002(data_dir):
    from . import sgf_table_sums

    file = "02state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_1992_2004.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_1992_2004[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2002"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2003(data_dir):
    from . import sgf_table_sums

    file = "03state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_1992_2004.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_1992_2004[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2003"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2004(data_dir):
    from . import sgf_table_sums

    file = "04state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_1992_2004.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_1992_2004[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2004"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2005(data_dir):
    from . import sgf_table_sums

    file = "05state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_2005_2011.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_2005_2011[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2005"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2006(data_dir):
    from . import sgf_table_sums

    file = "06state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_2005_2011.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_2005_2011[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2006"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2007(data_dir):
    from . import sgf_table_sums

    file = "07state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_2005_2011.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_2005_2011[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2007"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2008(data_dir):
    from . import sgf_table_sums

    file = "08state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_2005_2011.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_2005_2011[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2008"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2009(data_dir):
    from . import sgf_table_sums

    file = "09state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_2005_2011.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_2005_2011[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2009"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2010(data_dir):
    from . import sgf_table_sums

    file = "10state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_2005_2011.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_2005_2011[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2010"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2011(data_dir):
    from . import sgf_table_sums

    file = "11state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_2005_2011.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_2005_2011[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2011"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2012(data_dir):
    from . import sgf_table_sums

    file = "12state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_2012_2018.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_2012_2018[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2012"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2013(data_dir):
    from . import sgf_table_sums

    file = "13state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_2012_2018.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_2012_2018[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2013"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2014(data_dir):
    from . import sgf_table_sums

    file = "14state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_2012_2018.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_2012_2018[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2014"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2015(data_dir):
    from . import sgf_table_sums

    file = "15state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_2012_2018.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_2012_2018[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2015"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2016(data_dir):
    from . import sgf_table_sums

    file = "16state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_2012_2018.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_2012_2018[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2016"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2017(data_dir):
    from . import sgf_table_sums

    file = "17state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_2012_2018.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_2012_2018[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2017"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table


def _2018(data_dir):
    from . import sgf_table_sums

    file = "18state35.txt"

    ids = pd.read_excel(
        os.path.join(data_dir, "government-ids.xls"),
        dtype={"ID Code": str, "State": str},
    )

    ids["State"] = ids["State"].str.strip()

    map_id = dict(zip(ids["ID Code"], ids["State"]))
    map_id["00000000000000"] = "United States"
    map_id["09000000000000"] = "District of Columbia"

    t = pd.read_table(os.path.join(data_dir, file), header=None, index_col=None)

    t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
    t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
    t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
    t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
    t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
    t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]

    t["Amount"] = t["Amount"].map(int)
    t["Government Name"] = t["Government Code"].map(map_id)

    regions = list(set(t["Government Name"]))
    regions.sort()

    cols = ["Category"]
    cols.extend(regions)
    table = pd.DataFrame(columns=cols)

    for n, row in enumerate(sgf_table_sums.sums_2012_2018.keys()):
        table.loc[n, "Category"] = row

        for region in regions:
            table.loc[n, region] = t[
                (t["Government Name"] == region)
                & (t["Item Code"].isin(sgf_table_sums.sums_2012_2018[row]) == True)
            ]["Amount"].sum()

    table = pd.melt(table, id_vars="Category", var_name="State")
    table["year"] = "2018"
    table["units"] = "thousands of us dollars (USD)"

    # typing
    table["Category"] = table["Category"].map(str)
    table["State"] = table["State"].map(str)
    table["value"] = table["value"].map(int)
    table["year"] = table["year"].map(str)
    table["units"] = table["units"].map(str)

    return table
