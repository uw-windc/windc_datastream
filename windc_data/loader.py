import os
import pandas as pd
import numpy as np
import pprint
import copy
from windc_data import *
from terminaltables import SingleTable
from textwrap import wrap


class WindcEnvironment:

    _versions = {"windc_2_0_1": windc_2_0_1, "windc_2_1": windc_2_1}
    _load_options = {
        "windc_2_0_1": {
            "bea_use",
            "bea_supply",
            "cfs",
            "bea_use_det",
            "bea_supply_det",
            "eia_emissions",
            "eia_crude_price",
            "eia_seds",
            "eia_heatrate",
            "usda_nass",
            "bea_pce",
            "bea_gsp",
            "state_exim",
            "census_sgf",
            "emission_rate",
        },
        "windc_2_1": {
            "bea_use",
            "bea_supply",
            "cfs",
            "bea_use_det",
            "bea_supply_det",
            "eia_emissions",
            "eia_crude_price",
            "eia_seds",
            "eia_heatrate",
            "usda_nass",
            "bea_pce",
            "bea_gsp",
            "state_exim",
            "census_sgf",
            "emission_rate",
        },
    }

    def __init__(self, gams_sysdir, data_dir, version=None, load=None):
        if version is None:
            version = "windc_2_1"
        if version not in self._versions:
            raise ValueError(f"Unknown Windc version: {version}")

        if load is None:
            load = self._load_options[version]

        if not isinstance(load, (str, list, set)):
            raise ValueError("load argument must be of type str, list or set")

        if isinstance(load, str):
            load = [load]

        if set(load).issubset(self._load_options[version]):
            self._load_from = load
        else:
            raise ValueError(
                f"load arguments must be one or more of the following: {self._load_options[version]}"
            )

        self.gams_sysdir = os.path.abspath(gams_sysdir)
        self.version = version
        self.data_dir = os.path.abspath(os.path.expanduser(data_dir))

        print(f"GAMS sysdir given as: {self.gams_sysdir}")
        print(f"WiNDC data directory given as: {self.data_dir}")

        # create data container
        self.data = {}

        # load data
        self._load_data()

        # container for notation
        self.windc_notation = {}
        for k, v in mappings.maps.items():
            for col in v.columns:
                self.windc_notation[col] = set(mappings.maps[k][col])

        # container for column links
        # structure is {tablename:[(original_column_name, linked_notation_name)]}
        self.notation_link = {}

    def _load_data(self):
        # get data from version of windc
        for i in dir(self._versions[self.version].join):
            if (
                callable(getattr(self._versions[self.version].join, i))
                and i in self._load_from
            ):
                print(f"Loading data from... {i}")
                self.data[i] = getattr(self._versions[self.version].join, i)(
                    self.data_dir
                )

    def diff(self):
        pass

    def bulk_strip(self):
        for k, v in self.data.items():
            for col in v.columns:
                if v[col].dtype == "O":
                    v[col] = v[col].str.strip()

    def column_dtypes(self, dtypes):
        for k, v in self.data.items():
            for i in v.columns.to_list():
                print(
                    f"converting '{i}' in '{k}' from '{v[i].dtype}' --> '{dtypes[k][i]}'"
                )
                self.data[k][i] = self.data[k][i].map(dtypes[k][i])

    def bulk_replace(self, convert):
        for k, v in self.data.items():
            self.data[k].replace(convert, inplace=True)

    def remove_zeros(self):
        for k, v in self.data.items():
            for col in v.columns:

                if v[col].dtype == "float":
                    idx = self.data[k][self.data[k][col] == 0].index
                    if len(idx) != 0:
                        print(f"removing {len(idx)} zeros from '{col}' in '{k}'")
                        self.data[k].drop(idx, inplace=True)

            self.data[k].reset_index(drop=True, inplace=True)

    def drop_rows(self):
        for k, v in self.__to_drop__.items():
            for col, v2 in v.items():

                if v2 != []:
                    idx = self.data[k][self.data[k][col].isin(v2) == True].index
                    print(f"dropping {len(idx)} rows from '{col}' in '{k}'")
                    self.data[k].drop(idx, inplace=True)

        for k, v in self.data.items():
            self.data[k].reset_index(drop=True, inplace=True)

    def test_notation(self):
        self.__to_drop__ = {}

        for k in self.notation_link.keys():
            v = self.data[k]
            self.__to_drop__[k] = {}

            # look through all linked columns of the dataframe
            for d, nl in self.notation_link[k]:
                print(f"column name {d} linked to {nl}")
                self.__to_drop__[k][d] = []

                # set up new table for output display
                table_data = []
                table_title = f"{d} :: {k}"

                data = set(v[d])
                notation = self.windc_notation[nl]

                diff_data = data - notation
                diff_notation = notation - data

                # if sets are equal
                if data == notation:
                    table_data.append(["Valid dense data detected..."])
                    table_data.append(["{data} == {notation}"])
                    table = SingleTable(table_data)
                    table.title = table_title
                    print(table.table)

                # if data is a subset of notation
                elif data.issubset(notation) and data != notation:
                    print(
                        "Valid sparse data detected... ({data} is a proper subset of {notation})"
                    )

                    table_data.append(
                        [
                            f"{len(diff_notation)} Notation elements not in Data",
                            f"{len(diff_data)} Data elements not in Notation",
                        ]
                    )
                    table = SingleTable(table_data)
                    max_widths = table.column_widths

                    col1 = list(diff_notation)
                    col1.sort()
                    col1 = "\n".join(wrap(f"{col1}", 2 * max_widths[0]))

                    col2 = list(diff_data)
                    col2.sort()
                    col2 = "\n".join(wrap(f"{col2}", 2 * max_widths[1]))

                    table_data.append([col1, col2])

                    table.title = table_title
                    print(table.table)

                # if data is a superset of notation
                elif data.issuperset(notation) and data != notation:
                    self.__to_drop__[k][d].extend(diff_data)
                    print(
                        "**** Drop detected... ({data} is a proper superset of {notation})"
                    )

                    table_data.append(
                        [
                            f"{len(diff_notation)} Notation elements not in Data",
                            f"** {len(diff_data)} Drop Candidates **",
                        ]
                    )
                    table = SingleTable(table_data)
                    max_widths = table.column_widths

                    col1 = list(diff_notation)
                    col1.sort()
                    col1 = "\n".join(wrap(f"{col1}", 2 * max_widths[0]))

                    col2 = list(diff_data)
                    col2.sort()
                    col2 = "\n".join(wrap(f"{col2}", 2 * max_widths[1]))

                    table_data.append([col1, col2])

                    table.title = table_title
                    print(table.table)

                # if symmetric difference is not empty and the length of differences are ==
                elif data.symmetric_difference(notation) != set() and len(
                    diff_notation
                ) == len(diff_data):
                    self.__to_drop__[k][d].extend(diff_data)
                    print("**** Potential 1:1 map detected...")

                    table_data.append(
                        [
                            f"{len(diff_notation)} Notation elements not in Data",
                            f"{len(diff_data)} Data elements not in Notation",
                        ]
                    )
                    table = SingleTable(table_data)
                    max_widths = table.column_widths

                    col1 = list(diff_notation)
                    col1.sort()
                    col1 = "\n".join(wrap(f"{col1}", 2 * max_widths[0]))

                    col2 = list(diff_data)
                    col2.sort()
                    col2 = "\n".join(wrap(f"{col2}", 2 * max_widths[1]))

                    table_data.append([col1, col2])

                    table.title = table_title
                    print(table.table)

                # if the left and right differences are != and diff_notation not empty
                elif len(diff_notation) != len(diff_data) and diff_notation != set():
                    self.__to_drop__[k][d].extend(diff_data)

                    print("**** Drop detected (from a sparse data structure)... ")

                    table_data.append(
                        [
                            f"{len(diff_notation)} Notation elements not in Data",
                            f"** {len(diff_data)} Drop Candidates **",
                        ]
                    )
                    table = SingleTable(table_data)
                    max_widths = table.column_widths

                    col1 = list(diff_notation)
                    col1.sort()
                    col1 = "\n".join(wrap(f"{col1}", 2 * max_widths[0]))

                    col2 = list(diff_data)
                    col2.sort()
                    col2 = "\n".join(wrap(f"{col2}", 2 * max_widths[1]))

                    table_data.append([col1, col2])

                    table.title = table_title
                    print(table.table)

                else:
                    print(f"**** UNKNOWN ISSUE in notation link {k}")

                print("")

    def bulk_map_column(self, from_col, to_col, mapping):
        for k, v in self.data.items():
            if from_col in v.columns:
                print(f"mapping column '{to_col}' in table '{k}'...")
                self.data[k][to_col] = self.data[k][from_col].map(mapping)

    def unique_columns(self):
        col = set()
        for k, v in self.data.items():
            col.update(v.columns)
        return col

    def find_column(self, columns):
        pp = pprint.PrettyPrinter(indent=2)
        result = {}
        for i in columns:
            table = []
            for k, v in self.data.items():
                if i in set(v.columns):
                    table.append(k)
            result[i] = table
        return pp.pprint(result)

    def column_view(self):
        pp = pprint.PrettyPrinter(indent=2)
        cv = {}
        for k, v in self.data.items():
            cv[k] = v.columns.tolist()
        return pp.pprint(cv)

    def rebuild(self, gdxout=None):
        if gdxout == None:
            gdxout = False

        if not isinstance(gdxout, bool):
            raise Exception("gdxout must be type bool")

        if gdxout == False:
            print("rebuilding WiNDC Data without writing GDX file")
            self.clean()
            self.transform()
            self.remove_zeros()
            self.apply_gams_labels()

        if gdxout == True:
            print("rebuilding WiNDC Data.. GDX file will be created")
            print(
                f"GDX file will be written to: {os.path.join(os.getcwd(), 'windc_base.gdx')}"
            )

            self.clean()
            self.transform()
            self.remove_zeros()
            self.apply_gams_labels()
            self.export_gdx()

    def clean(self):
        if self.version == "windc_2_0_1":
            self.windc_notation["year"] = set([str(i) for i in range(1997, 2017)])

            self.bulk_strip()
            self.remove_zeros()

            dtypes = {
                "bea_supply": {
                    "IOCode": str,
                    "Row_Name": str,
                    "Commodities/Industries": str,
                    "Column_Name": str,
                    "value": float,
                    "year": str,
                    "units": str,
                },
                "bea_use": {
                    "IOCode": str,
                    "Row_Name": str,
                    "Commodities/Industries": str,
                    "Column_Name": str,
                    "value": float,
                    "year": str,
                    "units": str,
                },
                "bea_supply_det": {
                    "IOCode": str,
                    "Row_Name": str,
                    "Commodities/Industries": str,
                    "Column_Name": str,
                    "value": float,
                    "year": str,
                    "units": str,
                },
                "bea_use_det": {
                    "IOCode": str,
                    "Row_Name": str,
                    "Commodities/Industries": str,
                    "Column_Name": str,
                    "value": float,
                    "year": str,
                    "units": str,
                },
                "census_sgf": {
                    "Category": str,
                    "State": str,
                    "value": float,
                    "year": str,
                    "units": str,
                },
                "cfs": {
                    "SHIPMT_ID": str,
                    "ORIG_STATE": str,
                    "ORIG_MA": str,
                    "ORIG_CFS_AREA": str,
                    "DEST_STATE": str,
                    "DEST_MA": str,
                    "DEST_CFS_AREA": str,
                    "NAICS": str,
                    "QUARTER": str,
                    "SCTG": str,
                    "MODE": str,
                    "SHIPMT_VALUE": float,
                    "SHIPMT_WGHT": float,
                    "SHIPMT_DIST_GC": float,
                    "SHIPMT_DIST_ROUTED": float,
                    "TEMP_CNTL_YN": str,
                    "EXPORT_YN": str,
                    "EXPORT_CNTRY": str,
                    "HAZMAT": str,
                    "WGT_FACTOR": float,
                    "SHIPMT_VALUE_units": str,
                    "SHIPMT_WGHT_units": str,
                    "SHIPMT_DIST_GC_units": str,
                    "SHIPMT_DIST_ROUTED_units": str,
                    "year": str,
                },
                "bea_gsp": {
                    "GeoFIPS": str,
                    "GeoName": str,
                    "Region": str,
                    "TableName": str,
                    "ComponentName": str,
                    "Unit": str,
                    "IndustryId": str,
                    "IndustryClassification": str,
                    "Description": str,
                    "year": str,
                    "value": float,
                },
                "bea_pce": {
                    "GeoFIPS": str,
                    "GeoName": str,
                    "Region": str,
                    "TableName": str,
                    "ComponentName": str,
                    "Unit": str,
                    "Line": str,
                    "IndustryClassification": str,
                    "Description": str,
                    "year": str,
                    "value": float,
                },
                "eia_crude_price": {
                    "year": str,
                    "price": float,
                    "units": str,
                    "notes": str,
                },
                "eia_emissions": {
                    "State": str,
                    "year": str,
                    "emissions": float,
                    "units": str,
                    "sector": str,
                },
                "eia_heatrate": {
                    "year": str,
                    "units": str,
                    "variable": str,
                    "value": float,
                },
                "eia_seds": {
                    "Data_Status": str,
                    "MSN": str,
                    "StateCode": str,
                    "Year": str,
                    "Data": float,
                    "full_description": str,
                    "units": str,
                    "source": str,
                    "sector": str,
                },
                "state_exim": {
                    "State": str,
                    "Commodity": str,
                    "Country": str,
                    "Time": str,
                    "value": float,
                    "NAICS": str,
                    "Commodity Description": str,
                    "units": str,
                    "flow": str,
                },
                "usda_nass": {
                    "Program": str,
                    "Year": str,
                    "Period": str,
                    "Geo Level": str,
                    "State": str,
                    "State ANSI": str,
                    "watershed_code": str,
                    "Commodity": str,
                    "Data Item": str,
                    "Domain": str,
                    "Domain Category": str,
                    "Value": float,
                    "CV (%)": float,
                    "units": str,
                },
                "emission_rate": {"fuel": str, "units": str, "value": float,},
            }

            self.column_dtypes(dtypes)

            # create a new notation
            self.windc_notation["usda_naics"] = {
                "11111",
                "11112",
                "11113",
                "11114",
                "11115",
                "11116",
                "11119",
                "1112",
                "1113",
                "1114",
                "1119",
                "11211",
                "11212",
                "1122",
                "1123",
                "1124",
                "1125",
                "1129",
            }

            # create a new notation
            self.windc_notation["units"] = {
                "kilograms CO2 per million btu",
                "billion btu",
                "btu per kilowatthour",
                "million btu",
                "million btu per barrel",
                "million btu per short ton",
                "million cubic feet",
                "million kilowatthours",
                "million metric tons of carbon dioxide",
                "millions of chained (2009) us dollars (USD)",
                "millions of chained (2012) us dollars (USD)",
                "millions of current us dollars (USD)",
                "millions of us dollars (USD)",
                "percent",
                "percent change",
                "percentage points",
                "quantity index",
                "thousand",
                "thousand barrels",
                "thousand btu per chained (2009) us dollars (USD)",
                "thousand btu per cubic foot",
                "thousand btu per kilowatthour",
                "thousand cords",
                "thousand short tons",
                "thousands of us dollars (USD)",
                "us dollars (USD)",
                "us dollars (USD) per million btu",
                "us dollars (USD) per barrel",
            }

            # create a new notation from data
            cfs_ma = set(self.data["cfs"]["ORIG_MA"])
            cfs_ma.update(set(self.data["cfs"]["DEST_MA"]))
            cfs_ma = cfs_ma - {"0"}
            self.windc_notation["cfs_ma"] = cfs_ma

            # create a new notation from data
            # drop undisclosed SCTG data
            cfs_sctg = set(self.data["cfs"]["SCTG"])
            cfs_sctg = cfs_sctg - {
                "00",
                "25-30",
                "01-05",
                "15-19",
                "10-14",
                "06-09",
                "39-99",
                "20-24",
                "31-34",
                "35-38",
                "99",
            }
            self.windc_notation["cfs_sctg"] = cfs_sctg

            # create a new notation from data
            # drop undisclosed SCTG data
            self.windc_notation["cfs_export"] = {"N"}

            # create notation links
            self.notation_link["cfs"] = [
                ("ORIG_STATE", "fips.state"),
                ("DEST_STATE", "fips.state"),
                ("ORIG_MA", "cfs_ma"),
                ("DEST_MA", "cfs_ma"),
                ("SCTG", "cfs_sctg"),
                ("EXPORT_YN", "cfs_export"),
            ]

            # create a new notation from data
            gsp = set(self.data["bea_gsp"]["ComponentName"])
            gsp = gsp - {"Contributions to percent change in real GDP"}
            self.windc_notation["gsp_componentname"] = gsp

            # create a new notation from data
            self.windc_notation["pce_componentname"] = {
                "Total personal consumption expenditures (PCE) by state"
            }

            # create a new notation from data
            self.windc_notation["sgf_category"] = set(
                self.data["census_sgf"]["Category"]
            )

            self.windc_notation["exim_cnty"] = {"World Total"}

            self.notation_link["bea_supply"] = [("year", "year")]
            self.notation_link["bea_use"] = [("year", "year")]
            self.notation_link["bea_supply_det"] = [("year", "year")]
            self.notation_link["bea_use_det"] = [("year", "year")]

            self.notation_link["bea_gsp"] = [
                ("year", "year"),
                ("GeoName", "region.fullname"),
                ("Unit", "units"),
                ("ComponentName", "gsp_componentname"),
            ]
            self.notation_link["bea_pce"] = [
                ("year", "year"),
                ("GeoName", "region.fullname"),
                ("Unit", "units"),
                ("ComponentName", "pce_componentname"),
            ]
            self.notation_link["eia_crude_price"] = [
                ("year", "year"),
                ("units", "units"),
            ]
            self.notation_link["eia_emissions"] = [
                ("year", "year"),
                ("State", "region.fullname"),
                ("units", "units"),
            ]
            self.notation_link["eia_heatrate"] = [("year", "year"), ("units", "units")]
            self.notation_link["eia_seds"] = [
                ("Year", "year"),
                ("StateCode", "region.abbv"),
                ("units", "units"),
            ]
            self.notation_link["state_exim"] = [
                ("State", "region.fullname"),
                ("Time", "year"),
                ("units", "units"),
                ("Country", "exim_cnty"),
            ]
            self.notation_link["usda_nass"] = [
                ("Year", "year"),
                ("State", "region.fullname"),
                ("Domain Category", "usda_naics"),
                ("units", "units"),
            ]

            self.notation_link["census_sgf"] = [
                ("year", "year"),
                ("State", "region.fullname"),
                ("units", "units"),
                ("Category", "sgf_category"),
            ]

            self.bulk_replace(
                {
                    "ALABAMA": "Alabama",
                    "ALASKA": "Alaska",
                    "ARIZONA": "Arizona",
                    "ARKANSAS": "Arkansas",
                    "CALIFORNIA": "California",
                    "COLORADO": "Colorado",
                    "CONNECTICUT": "Connecticut",
                    "DELAWARE": "Delaware",
                    "DISTRICT OF COLUMBIA": "District of Columbia",
                    "FLORIDA": "Florida",
                    "GEORGIA": "Georgia",
                    "HAWAII": "Hawaii",
                    "IDAHO": "Idaho",
                    "ILLINOIS": "Illinois",
                    "INDIANA": "Indiana",
                    "IOWA": "Iowa",
                    "KANSAS": "Kansas",
                    "KENTUCKY": "Kentucky",
                    "LOUISIANA": "Louisiana",
                    "MAINE": "Maine",
                    "MARYLAND": "Maryland",
                    "MASSACHUSETTS": "Massachusetts",
                    "MICHIGAN": "Michigan",
                    "MINNESOTA": "Minnesota",
                    "MISSISSIPPI": "Mississippi",
                    "MISSOURI": "Missouri",
                    "MONTANA": "Montana",
                    "NEBRASKA": "Nebraska",
                    "NEVADA": "Nevada",
                    "NEW HAMPSHIRE": "New Hampshire",
                    "NEW JERSEY": "New Jersey",
                    "NEW MEXICO": "New Mexico",
                    "NEW YORK": "New York",
                    "NORTH CAROLINA": "North Carolina",
                    "NORTH DAKOTA": "North Dakota",
                    "OHIO": "Ohio",
                    "OKLAHOMA": "Oklahoma",
                    "OREGON": "Oregon",
                    "PENNSYLVANIA": "Pennsylvania",
                    "RHODE ISLAND": "Rhode Island",
                    "SOUTH CAROLINA": "South Carolina",
                    "SOUTH DAKOTA": "South Dakota",
                    "TENNESSEE": "Tennessee",
                    "TEXAS": "Texas",
                    "UTAH": "Utah",
                    "UNITED STATES": "United States",
                    "VERMONT": "Vermont",
                    "VIRGINIA": "Virginia",
                    "WASHINGTON": "Washington",
                    "WEST VIRGINIA": "West Virginia",
                    "WISCONSIN": "Wisconsin",
                    "WYOMING": "Wyoming",
                    "Dist of Columbia": "District of Columbia",
                    "State total (unadjusted)": "United States",
                    "Dollars": "us dollars (USD)",
                    "Percent change": "percent change",
                    "Thousand Btu per cubic feet": "thousand btu per cubic foot",
                    "Thousand": "thousand",
                    "Quantity index": "quantity index",
                    "Percent": "percent",
                    "Dollars per million Btu": "us dollars (USD) per million btu",
                    "Billion Btu": "billion btu",
                    "Thousand Btu per kilowatthour": "thousand btu per kilowatthour",
                    "Million Btu per short ton": "million btu per short ton",
                    "Thousand short tons": "thousand short tons",
                    "Million cubic feet": "million cubic feet",
                    "Million chained (2009) dollars": "millions of chained (2009) us dollars (USD)",
                    "Thousand cords": "thousand cords",
                    "Thousand Btu per chained (2009) dollar": "thousand btu per chained (2009) us dollars (USD)",
                    "Percentage points": "percentage points",
                    "Million kilowatthours": "million kilowatthours",
                    "Million dollars": "millions of us dollars (USD)",
                    "Thousand Btu per cubic foot": "thousand btu per cubic foot",
                    "btu per kWh generated": "btu per kilowatthour",
                    "Thousands of dollars": "thousands of us dollars (USD)",
                    "Million Btu per barrel": "million btu per barrel",
                    "Million Btu": "million btu",
                    "Millions of chained 2012 dollars": "millions of chained (2012) us dollars (USD)",
                    "Millions of current dollars": "millions of us dollars (USD)",
                    "Thousand barrels": "thousand barrels",
                }
            )
            self.test_notation()
            self.drop_rows()

            # remap regions to defaults
            self.remap()

        if self.version == "windc_2_1":
            self.windc_notation["year"] = set([str(i) for i in range(1997, 2018)])

            self.bulk_strip()
            self.remove_zeros()

            self.data["bea_gsp"].rename(
                columns={"LineCode": "IndustryId"}, inplace=True
            )

            dtypes = {
                "bea_supply": {
                    "IOCode": str,
                    "Row_Name": str,
                    "Commodities/Industries": str,
                    "Column_Name": str,
                    "value": float,
                    "year": str,
                    "units": str,
                },
                "bea_use": {
                    "IOCode": str,
                    "Row_Name": str,
                    "Commodities/Industries": str,
                    "Column_Name": str,
                    "value": float,
                    "year": str,
                    "units": str,
                },
                "bea_supply_det": {
                    "IOCode": str,
                    "Row_Name": str,
                    "Commodities/Industries": str,
                    "Column_Name": str,
                    "value": float,
                    "year": str,
                    "units": str,
                },
                "bea_use_det": {
                    "IOCode": str,
                    "Row_Name": str,
                    "Commodities/Industries": str,
                    "Column_Name": str,
                    "value": float,
                    "year": str,
                    "units": str,
                },
                "census_sgf": {
                    "Category": str,
                    "State": str,
                    "value": float,
                    "year": str,
                    "units": str,
                },
                "cfs": {
                    "SHIPMT_ID": str,
                    "ORIG_STATE": str,
                    "ORIG_MA": str,
                    "ORIG_CFS_AREA": str,
                    "DEST_STATE": str,
                    "DEST_MA": str,
                    "DEST_CFS_AREA": str,
                    "NAICS": str,
                    "QUARTER": str,
                    "SCTG": str,
                    "MODE": str,
                    "SHIPMT_VALUE": float,
                    "SHIPMT_WGHT": float,
                    "SHIPMT_DIST_GC": float,
                    "SHIPMT_DIST_ROUTED": float,
                    "TEMP_CNTL_YN": str,
                    "EXPORT_YN": str,
                    "EXPORT_CNTRY": str,
                    "HAZMAT": str,
                    "WGT_FACTOR": float,
                    "SHIPMT_VALUE_units": str,
                    "SHIPMT_WGHT_units": str,
                    "SHIPMT_DIST_GC_units": str,
                    "SHIPMT_DIST_ROUTED_units": str,
                    "year": str,
                },
                "bea_gsp": {
                    "GeoFIPS": str,
                    "GeoName": str,
                    "Region": str,
                    "TableName": str,
                    "ComponentName": str,
                    "Unit": str,
                    "IndustryId": str,
                    "IndustryClassification": str,
                    "Description": str,
                    "year": str,
                    "value": float,
                },
                "bea_pce": {
                    "GeoFIPS": str,
                    "GeoName": str,
                    "Region": str,
                    "TableName": str,
                    "ComponentName": str,
                    "Unit": str,
                    "Line": str,
                    "IndustryClassification": str,
                    "Description": str,
                    "year": str,
                    "value": float,
                },
                "eia_crude_price": {
                    "year": str,
                    "price": float,
                    "units": str,
                    "notes": str,
                },
                "eia_emissions": {
                    "State": str,
                    "year": str,
                    "emissions": float,
                    "units": str,
                    "sector": str,
                },
                "eia_heatrate": {
                    "year": str,
                    "units": str,
                    "variable": str,
                    "value": float,
                },
                "eia_seds": {
                    "Data_Status": str,
                    "MSN": str,
                    "StateCode": str,
                    "Year": str,
                    "Data": float,
                    "full_description": str,
                    "units": str,
                    "source": str,
                    "sector": str,
                },
                "state_exim": {
                    "State": str,
                    "Commodity": str,
                    "Country": str,
                    "Time": str,
                    "value": float,
                    "NAICS": str,
                    "Commodity Description": str,
                    "units": str,
                    "flow": str,
                },
                "usda_nass": {
                    "Program": str,
                    "Year": str,
                    "Period": str,
                    "Geo Level": str,
                    "State": str,
                    "State ANSI": str,
                    "watershed_code": str,
                    "Commodity": str,
                    "Data Item": str,
                    "Domain": str,
                    "Domain Category": str,
                    "Value": float,
                    "CV (%)": float,
                    "units": str,
                },
                "emission_rate": {"fuel": str, "units": str, "value": float,},
            }

            self.column_dtypes(dtypes)

            self.windc_notation["usda_naics"] = {
                "11111",
                "11112",
                "11113",
                "11114",
                "11115",
                "11116",
                "11119",
                "1112",
                "1113",
                "1114",
                "1119",
                "11211",
                "11212",
                "1122",
                "1123",
                "1124",
                "1125",
                "1129",
            }

            # create a new notation
            self.windc_notation["units"] = {
                "kilowatthours",
                "barrels",
                "kilograms CO2 per million btu",
                "billion btu",
                "btu per kilowatthour",
                "million btu",
                "million btu per barrel",
                "million btu per short ton",
                "million cubic feet",
                "million kilowatthours",
                "million metric tons of carbon dioxide",
                "millions of chained (2009) us dollars (USD)",
                "millions of chained (2012) us dollars (USD)",
                "millions of current us dollars (USD)",
                "millions of us dollars (USD)",
                "percent",
                "percent change",
                "percentage points",
                "quantity index",
                "thousand",
                "thousand barrels",
                "thousand btu per chained (2009) us dollars (USD)",
                "thousand btu per chained (2012) us dollars (USD)",
                "thousand btu per cubic foot",
                "thousand btu per kilowatthour",
                "thousand cords",
                "thousand cubic feet",
                "thousand short tons",
                "thousands of us dollars (USD)",
                "us dollars (USD)",
                "us dollars (USD) per million btu",
                "us dollars (USD) per barrel",
            }

            # create a new notation from data
            cfs_ma = set(self.data["cfs"]["ORIG_MA"])
            cfs_ma.update(set(self.data["cfs"]["DEST_MA"]))
            cfs_ma = cfs_ma - {"0"}
            self.windc_notation["cfs_ma"] = cfs_ma

            # create a new notation from data
            # drop undisclosed SCTG data
            cfs_sctg = set(self.data["cfs"]["SCTG"])
            cfs_sctg = cfs_sctg - {
                "00",
                "25-30",
                "01-05",
                "15-19",
                "10-14",
                "06-09",
                "39-99",
                "20-24",
                "31-34",
                "35-38",
                "99",
            }
            self.windc_notation["cfs_sctg"] = cfs_sctg

            # create a new notation from data
            # drop undisclosed SCTG data
            self.windc_notation["cfs_export"] = {"N"}

            # create notation links
            self.notation_link["cfs"] = [
                ("ORIG_STATE", "fips.state"),
                ("DEST_STATE", "fips.state"),
                ("ORIG_MA", "cfs_ma"),
                ("DEST_MA", "cfs_ma"),
                ("SCTG", "cfs_sctg"),
                ("EXPORT_YN", "cfs_export"),
            ]

            # create a new notation from data
            gsp = set(self.data["bea_gsp"]["ComponentName"])
            gsp = gsp - {"Contributions to percent change in real GDP"}
            self.windc_notation["gsp_componentname"] = gsp

            # create a new notation from data
            self.windc_notation["pce_componentname"] = {
                "Total personal consumption expenditures (PCE) by state"
            }

            # create a new notation from data
            self.windc_notation["sgf_category"] = set(
                self.data["census_sgf"]["Category"]
            )

            self.windc_notation["exim_cnty"] = {"World Total"}

            self.notation_link["bea_supply"] = [("year", "year")]
            self.notation_link["bea_use"] = [("year", "year")]
            self.notation_link["bea_supply_det"] = [("year", "year")]
            self.notation_link["bea_use_det"] = [("year", "year")]

            self.notation_link["bea_gsp"] = [
                ("year", "year"),
                ("GeoName", "region.fullname"),
                ("Unit", "units"),
                ("ComponentName", "gsp_componentname"),
            ]
            self.notation_link["bea_pce"] = [
                ("year", "year"),
                ("GeoName", "region.fullname"),
                ("Unit", "units"),
                ("ComponentName", "pce_componentname"),
            ]
            self.notation_link["eia_crude_price"] = [
                ("year", "year"),
                ("units", "units"),
            ]
            self.notation_link["eia_emissions"] = [
                ("year", "year"),
                ("State", "region.fullname"),
                ("units", "units"),
            ]
            self.notation_link["eia_heatrate"] = [("year", "year"), ("units", "units")]
            self.notation_link["eia_seds"] = [
                ("Year", "year"),
                ("StateCode", "region.abbv"),
                ("units", "units"),
            ]
            self.notation_link["state_exim"] = [
                ("State", "region.fullname"),
                ("Time", "year"),
                ("units", "units"),
                ("Country", "exim_cnty"),
            ]
            self.notation_link["usda_nass"] = [
                ("Year", "year"),
                ("State", "region.fullname"),
                ("Domain Category", "usda_naics"),
                ("units", "units"),
            ]

            self.notation_link["census_sgf"] = [
                ("year", "year"),
                ("State", "region.fullname"),
                ("units", "units"),
                ("Category", "sgf_category"),
            ]

            self.bulk_replace(
                {
                    "ALABAMA": "Alabama",
                    "ALASKA": "Alaska",
                    "ARIZONA": "Arizona",
                    "ARKANSAS": "Arkansas",
                    "CALIFORNIA": "California",
                    "COLORADO": "Colorado",
                    "CONNECTICUT": "Connecticut",
                    "DELAWARE": "Delaware",
                    "DISTRICT OF COLUMBIA": "District of Columbia",
                    "FLORIDA": "Florida",
                    "GEORGIA": "Georgia",
                    "HAWAII": "Hawaii",
                    "IDAHO": "Idaho",
                    "ILLINOIS": "Illinois",
                    "INDIANA": "Indiana",
                    "IOWA": "Iowa",
                    "KANSAS": "Kansas",
                    "KENTUCKY": "Kentucky",
                    "LOUISIANA": "Louisiana",
                    "MAINE": "Maine",
                    "MARYLAND": "Maryland",
                    "MASSACHUSETTS": "Massachusetts",
                    "MICHIGAN": "Michigan",
                    "MINNESOTA": "Minnesota",
                    "MISSISSIPPI": "Mississippi",
                    "MISSOURI": "Missouri",
                    "MONTANA": "Montana",
                    "NEBRASKA": "Nebraska",
                    "NEVADA": "Nevada",
                    "NEW HAMPSHIRE": "New Hampshire",
                    "NEW JERSEY": "New Jersey",
                    "NEW MEXICO": "New Mexico",
                    "NEW YORK": "New York",
                    "NORTH CAROLINA": "North Carolina",
                    "NORTH DAKOTA": "North Dakota",
                    "OHIO": "Ohio",
                    "OKLAHOMA": "Oklahoma",
                    "OREGON": "Oregon",
                    "PENNSYLVANIA": "Pennsylvania",
                    "RHODE ISLAND": "Rhode Island",
                    "SOUTH CAROLINA": "South Carolina",
                    "SOUTH DAKOTA": "South Dakota",
                    "TENNESSEE": "Tennessee",
                    "TEXAS": "Texas",
                    "UTAH": "Utah",
                    "UNITED STATES": "United States",
                    "VERMONT": "Vermont",
                    "VIRGINIA": "Virginia",
                    "WASHINGTON": "Washington",
                    "WEST VIRGINIA": "West Virginia",
                    "WISCONSIN": "Wisconsin",
                    "WYOMING": "Wyoming",
                    "Dist of Columbia": "District of Columbia",
                    "State total (unadjusted)": "United States",
                    "Dollars": "us dollars (USD)",
                    "Percent change": "percent change",
                    "Thousand Btu per cubic feet": "thousand btu per cubic foot",
                    "Thousand": "thousand",
                    "Quantity index": "quantity index",
                    "Percent": "percent",
                    "Dollars per million Btu": "us dollars (USD) per million btu",
                    "Billion Btu": "billion btu",
                    "Thousand Btu per kilowatthour": "thousand btu per kilowatthour",
                    "Million Btu per short ton": "million btu per short ton",
                    "Thousand short tons": "thousand short tons",
                    "Million cubic feet": "million cubic feet",
                    "Million chained (2009) dollars": "millions of chained (2009) us dollars (USD)",
                    "Thousand cords": "thousand cords",
                    "Thousand Btu per chained (2009) dollar": "thousand btu per chained (2009) us dollars (USD)",
                    "Percentage points": "percentage points",
                    "Million kilowatthours": "million kilowatthours",
                    "Million dollars": "millions of us dollars (USD)",
                    "Thousand Btu per cubic foot": "thousand btu per cubic foot",
                    "btu per kWh generated": "btu per kilowatthour",
                    "Thousands of dollars": "thousands of us dollars (USD)",
                    "Million Btu per barrel": "million btu per barrel",
                    "Million Btu": "million btu",
                    "Millions of chained 2012 dollars": "millions of chained (2012) us dollars (USD)",
                    "Millions of current dollars": "millions of us dollars (USD)",
                    "Thousand barrels": "thousand barrels",
                    "Barrels": "barrels",
                    "Kilowatthours": "kilowatthours",
                    "Million chained (2012) dollars": "millions of chained (2012) us dollars (USD)",
                    "Thousand Btu per chained (2012) dollar": "thousand btu per chained (2012) us dollars (USD)",
                    "Thousand cubic feet": "thousand cubic feet",
                }
            )

            self.test_notation()
            self.drop_rows()

            # remap regions to defaults
            self.remap()

    def remap(self):
        if self.version in {"windc_2_0_1", "windc_2_1"}:
            for k in self.notation_link:
                for d, nl in self.notation_link[k]:
                    for kk, vv in mappings.maps.items():

                        if nl in vv.keys() and nl != mappings.default[kk]:
                            print(
                                f"'{d}' :: '{k}' is linked to '{nl}' and is being remapped to '{mappings.default[kk]}'"
                            )

                            # remap to default format
                            self.data[k][d] = self.data[k][d].map(
                                dict(
                                    zip(
                                        mappings.maps[kk][nl],
                                        mappings.maps[kk][mappings.default[kk]],
                                    )
                                )
                            )

            # add new notation links and keep track of ones to remove in the next step
            to_remove = {}
            for k in self.notation_link.keys():
                to_remove[k] = []
                for d, nl in self.notation_link[k]:
                    for kk, vv in mappings.maps.items():

                        if nl in vv.keys() and nl != mappings.default[kk]:
                            to_remove[k].append((d, nl))
                            self.notation_link[k].append((d, mappings.default[kk]))

            # remove old notation links
            for k in to_remove.keys():
                for d, nl in to_remove[k]:
                    self.notation_link[k].pop(self.notation_link[k].index((d, nl)))

    def transform(self):
        if self.version == "windc_2_0_1":
            from .windc_2_0_1 import xfrm

            # transform CFS data
            self.data["cfs_st"], self.data["cfs_ma"] = xfrm.cfs(self.data["cfs"])

            # transform bea_gsp data
            self.data["bea_gsp"] = xfrm.gsp(self.data["bea_gsp"])

            # transform usda_nass data
            self.data["usda_nass"] = xfrm.usda_nass(self.data["usda_nass"])

            # transform census_sgf data
            self.data["census_sgf"] = xfrm.census_sgf(self.data["census_sgf"])

            # transform state_exim data
            self.data["state_exim"] = xfrm.state_exim(self.data["state_exim"])

            # transform eia_heatrate data within class to allow access to some data
            # fill years in notation but not in data with 2005 values
            df = [self.data["eia_heatrate"]]
            for i in self.windc_notation["year"]:
                if i not in set(self.data["eia_heatrate"]["year"]):
                    t = self.data["eia_heatrate"][
                        self.data["eia_heatrate"]["year"] == "2005"
                    ].copy()
                    t["year"] = i
                    df.append(t)

            df = pd.concat(df, ignore_index=True)
            self.data["eia_heatrate"] = copy.deepcopy(df)

        if self.version == "windc_2_1":
            from .windc_2_1 import xfrm

            # transform CFS data
            self.data["cfs_st"], self.data["cfs_ma"] = xfrm.cfs(self.data["cfs"])

            # transform bea_gsp data
            self.data["bea_gsp"] = xfrm.gsp(self.data["bea_gsp"])

            # transform usda_nass data
            self.data["usda_nass"] = xfrm.usda_nass(self.data["usda_nass"])

            # transform census_sgf data
            self.data["census_sgf"] = xfrm.census_sgf(self.data["census_sgf"])

            # transform state_exim data
            self.data["state_exim"] = xfrm.state_exim(self.data["state_exim"])

            # transform eia_heatrate data within class to allow access to some data
            # fill years in notation but not in data with 2005 values
            df = [self.data["eia_heatrate"]]
            for i in self.windc_notation["year"]:
                if i not in set(self.data["eia_heatrate"]["year"]):
                    t = self.data["eia_heatrate"][
                        self.data["eia_heatrate"]["year"] == "2005"
                    ].copy()
                    t["year"] = i
                    df.append(t)

            df = pd.concat(df, ignore_index=True)
            self.data["eia_heatrate"] = copy.deepcopy(df)

    def apply_gams_labels(self):
        if self.version in {"windc_2_0_1", "windc_2_1"}:
            # eia_emissions
            self.data["eia_emissions"]["gams.sector"] = self.data["eia_emissions"][
                "sector"
            ].map(
                dict(
                    zip(
                        gams_windc.gams_maps["eia_emissions"]["eia_sector"],
                        gams_windc.gams_maps["eia_emissions"]["windc_label"],
                    )
                )
            )

            # bea_gsp
            self.data["bea_gsp"]["gams.ComponentName"] = self.data["bea_gsp"][
                "ComponentName"
            ].map(
                dict(
                    zip(
                        gams_windc.gams_maps["bea_gsp"]["bea_code"],
                        gams_windc.gams_maps["bea_gsp"]["windc_label"],
                    )
                )
            )

            # eia_heatrate
            self.data["eia_heatrate"]["gams.variable"] = self.data["eia_heatrate"][
                "variable"
            ].map(
                dict(
                    zip(
                        gams_windc.gams_maps["eia_gen"]["eia_technologies"],
                        gams_windc.gams_maps["eia_gen"]["windc_label"],
                    )
                )
            )

            # bea_pce
            self.data["bea_pce"]["gams.Description"] = self.data["bea_pce"][
                "Description"
            ].map(
                dict(
                    zip(
                        gams_windc.gams_maps["bea_pce"]["pce_description"],
                        gams_windc.gams_maps["bea_pce"]["windc_label"],
                    )
                )
            )

            # census_sgf
            self.data["census_sgf"]["gams.Category"] = self.data["census_sgf"][
                "Category"
            ].map(
                dict(
                    zip(
                        gams_windc.gams_maps["census_sgf"]["sgf_category"],
                        gams_windc.gams_maps["census_sgf"]["windc_label"],
                    )
                )
            )

            # bea_supply_det
            self.data["bea_supply_det"]["gams.IOCode"] = self.data["bea_supply_det"][
                "IOCode"
            ].map(
                dict(
                    zip(
                        gams_windc.gams_maps["bea_all_det"]["bea_code"],
                        gams_windc.gams_maps["bea_all_det"]["windc_label"],
                    )
                )
            )

            self.data["bea_supply_det"]["gams.Commodities/Industries"] = self.data[
                "bea_supply_det"
            ]["Commodities/Industries"].map(
                dict(
                    zip(
                        gams_windc.gams_maps["bea_all_det"]["bea_code"],
                        gams_windc.gams_maps["bea_all_det"]["windc_label"],
                    )
                )
            )

            # bea_use_det
            self.data["bea_use_det"]["gams.IOCode"] = self.data["bea_use_det"][
                "IOCode"
            ].map(
                dict(
                    zip(
                        gams_windc.gams_maps["bea_all_det"]["bea_code"],
                        gams_windc.gams_maps["bea_all_det"]["windc_label"],
                    )
                )
            )

            self.data["bea_use_det"]["gams.Commodities/Industries"] = self.data[
                "bea_use_det"
            ]["Commodities/Industries"].map(
                dict(
                    zip(
                        gams_windc.gams_maps["bea_all_det"]["bea_code"],
                        gams_windc.gams_maps["bea_all_det"]["windc_label"],
                    )
                )
            )

            # bea_supply
            self.data["bea_supply"]["gams.IOCode"] = self.data["bea_supply"][
                "IOCode"
            ].map(
                dict(
                    zip(
                        gams_windc.gams_maps["bea_all"]["bea_code"],
                        gams_windc.gams_maps["bea_all"]["windc_label"],
                    )
                )
            )

            self.data["bea_supply"]["gams.Commodities/Industries"] = self.data[
                "bea_supply"
            ]["Commodities/Industries"].map(
                dict(
                    zip(
                        gams_windc.gams_maps["bea_all"]["bea_code"],
                        gams_windc.gams_maps["bea_all"]["windc_label"],
                    )
                )
            )

            # bea_use
            self.data["bea_use"]["gams.IOCode"] = self.data["bea_use"]["IOCode"].map(
                dict(
                    zip(
                        gams_windc.gams_maps["bea_all"]["bea_code"],
                        gams_windc.gams_maps["bea_all"]["windc_label"],
                    )
                )
            )

            self.data["bea_use"]["gams.Commodities/Industries"] = self.data["bea_use"][
                "Commodities/Industries"
            ].map(
                dict(
                    zip(
                        gams_windc.gams_maps["bea_all"]["bea_code"],
                        gams_windc.gams_maps["bea_all"]["windc_label"],
                    )
                )
            )

    def set_gather(self, columns):
        t = set()
        for col in columns:
            for k, v in self.data.items():
                if col in v.columns:
                    t.update(set(v[col]))

        # remove any nan values from returned set
        return t - {np.nan}

    def gdx_data(self):
        if self.version == "windc_2_0_1":
            from .windc_2_0_1 import gdx_data

            return gdx_data.build_data(self.version, self.windc_notation, self.data)

        if self.version == "windc_2_1":
            from .windc_2_1 import gdx_data

            return gdx_data.build_data(self.version, self.windc_notation, self.data)

    def export_gdx(self):
        from windc_data.gmsxfr import GdxContainer

        # write gdx
        gdx = GdxContainer(self.gams_sysdir)
        gdx.add_to_gdx(
            self.gdx_data(), standardize_data=True, inplace=True, quality_checks=False
        )

        gdx.write_gdx(
            os.path.join(os.getcwd(), "windc_base.gdx"), compress=True,
        )

    def to_csv(self, output_dir=None):
        if output_dir == None:
            self.output_dir = os.path.join(os.getcwd(), "standarized_data")
        else:
            self.output_dir = os.path.abspath(output_dir)

        if os.path.isdir(self.output_dir) == False:
            os.mkdir(self.output_dir)

        for k, v in self.data.items():
            v.to_csv(os.path.join(self.output_dir, f"{k}.csv"))
