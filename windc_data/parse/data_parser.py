# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 07:54:19 2022

@author: Mitch
"""
import pandas as pd
import os

from . import mappings as mappings

from .gams_maps import GamsMaps

from terminaltables import SingleTable
from textwrap import wrap





REPLACE_DICT =                 {
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





class Parser:
    
    """
    Parent class to all data loading and cleaning. 
    
    Inputs:
        
        data_dir - The parent directory of the data. This will look something like
                   r"C:\path\to\data\windc_3_0_0"
                   
        years - Defaults to (1997,2020). Data outside of this range will be dropped.
                   
        verbose - Optional argument, defaults to True. If false will hide all the 
                  printed output.
                  
        data_info - A dictionary, defaults to {}. Typically this is the data stored
                    in a JSON file located in the raw_data directory. For the existing
                    datasets this has two keys "data" and "columns". 
                    
                    "data" -> Values are information about directory structure and 
                    where to find each dataset
                    
                    "columns" -> rename information. 
                  
                  
    There are a few very important variables initialized:
        
        self.windc_notation: key   -> linked column name
                             value -> Valid values for the column
                             
        self.notation_link: key   -> column name
                            value -> linked column name, corresponds to self.windc_notation
                            
        The primary reason for two dictionaries is that the data tends to be inconsistent
        across sources. This allows for the general building of windc_notation, providing
        consistent outputs, and linking the columns of a dataset.
        
        self.data: key   -> Up to user, best to describe loaded file
                   value -> A raw dataframe just loaded into memory
                   
        Each subclass will build this dictionary in the self.__load_data method
        and then use it to clean each loaded dataset.
        
        self.df: The joined and cleaned dataframe. This is the "primary" dataframe,
        although there can be more.
        
        self.gams_maps: Load the translations necessary to import to Gams.
    """
    
    def __init__(self,
                 data_dir,
                 gams_maps,
                 years = (1997,2020),
                 verbose = True,
                 data_info = {}):
        
        self.data_dir = data_dir
        self.verbose = verbose
        
        self.data_info = data_info
        
        self.data = {}
        self.notation_link = []
        
        self.windc_notation = {}
        for k, v in mappings.maps.items():
            for col in v.columns:
                self.windc_notation[col] = set(mappings.maps[k][col])
        
        
        self.gams_maps = gams_maps
        
        self.windc_notation["year"] = set([str(i) for i in range(years[0], years[1]+1)]) #The years of the data
        
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
        
        
        self._load_data()
        self.clean()
        
        self.bulk_strip()
        

        
        
        self.join()
    
    

    
        self.remove_zeros()
        
        self.bulk_replace(REPLACE_DICT)
    
        
        self.build_notations()
        
        self.test_notation()
        self.drop_rows()
        self.remap()
        
        self.transform()
        
        
    
    def _load_data(self):
        """
        Overwrite to implement loading the data. Typicall a call to pd.read_csv.
        
        When loading the data, load into the dictionary self.data with some key,
        the programs here will eventually join this dictionary into a single 
        data frame called self.df
        """
        pass
    
    def clean(self):
        """
        Overwrite to clean each dataset. The dictionary structure helps clean
        individual datasets.
        """
        pass
  
    def transform(self):
        """
        Overwrite to make final data transformations. This is the final step
        in preparing the data. Run this to make last minute modifications to
        the entire data frame. You should be modifying self.df here.
        """

        pass
  
    def build_notations(self):
        """
        Overwrite to build the dictionaries
            self.windc_notations
            self.notation_link
        These dictionaries ensure a consistent formatting of data across 
        datasets. 
        """
        pass    
  
    
    def get_df(self):
        return self.df
    
    def join(self):
        try:
            self.df = pd.concat(self.data.values(), ignore_index=True)
        except ValueError:
            self.df = pd.DataFrame()
 
    
    def remove_zeros(self):
        #for k, v in self.data.items():
            for col in self.df.columns:

                if self.df[col].dtype == "float":
                    idx = self.df[self.df[col] == 0].index
                    if len(idx) != 0:
                        if self.verbose:
                            print(f"removing {len(idx)} zeros from '{col}'")
                        self.df.drop(idx, inplace=True)

            self.df.reset_index(drop=True, inplace=True)

            
    def bulk_strip(self):
        for k, v in self.data.items():
            for col in v.columns:
                if v[col].dtype == "O":
                    v[col] = v[col].str.strip()
                    
                    

    def column_dtypes(self, dtypes):
        for k, v in self.data.items():
            for i in v.columns.to_list():
                if self.verbose:
                    print(
                        f"converting '{i}' in '{k}' from '{v[i].dtype}' --> '{dtypes[k][i]}'"
                        )
                self.data[k][i] = self.data[k][i].map(dtypes[k][i])               

    def bulk_replace(self, convert):
        #for k, v in self.data.items():
        self.df.replace(convert, inplace=True)                  
                    
        
        
    def drop_rows(self):
        for col, v2 in self.__to_drop__.items():


            if v2 != []:
                idx = self.df[self.df[col].isin(v2) == True].index
                if self.verbose:
                    print(f"dropping {len(idx)} rows from '{col}'")
                self.df.drop(idx, inplace=True)

        self.df.reset_index(drop=True, inplace=True)                
                    
            

      

    def to_csv(self,output_dir,output_name):
        
        if output_name[-4:]!=".csv":
            output_name += ".csv"
        
        self.df.to_csv(os.path.join(output_dir,output_name),index = False)
        
        

        

    def _build_gdx_dict(self):
        """
        Overwrite to build the dictionary that creates the gams output. This should
        return a dictionary with key the Gams name and value a diction of the form
    
        "type" -> Either "set" or "parameter"
        "elements" -> The records for gams, for parameters ensure the "value" 
                      column is the last in the dataframe.
        "text" -> A description of the data
        """
        
        pass

    


    def build_gdx(self,gdx_container):
        #gdx_dict = self.__initialize_gdx_dict()
        
        gdx_dict = self._build_gdx_dict()
        
        if gdx_dict is None:
            raise NotImplementedError("_build_gdx_dict is not implemented")
        
        for key,d in [(k,d) for (k,d) in gdx_dict.items() if d['type'] == 'set']:
            
            if key not in gdx_container.data:
                num_domain = 1
                if type(d['elements']) == type(pd.DataFrame()):
                    num_domain = len(d['elements'].columns)-1
                gdx_container.addSet(key,["*"]*num_domain,records = d['elements'],description = d['text'])
            
        for key,d in [(k,d) for (k,d) in gdx_dict.items() if d['type'] == 'parameter']:
            num_domain = len(d['elements'].columns)-1
            if key not in gdx_container.data:
                #["*"]*num_domain
                gdx_container.addParameter(key,d["domain"],records = d['elements'],description = d['text'])
        
        


    def to_gdx(self,output_dir,output_name = "windc_gams.gdx"):
        import gamstransfer as gt
        m = gt.Container()
        
        self.build_gdx(m)
        m.write(os.path.join(output_dir,output_name))



    def remap(self):
        #if self.version in {"windc_2_0_1", "windc_2_1"}:
        #    for k in self.notation_link:
        for d, nl in self.notation_link:
            for kk, vv in mappings.maps.items():

                if nl in vv.keys() and nl != mappings.default[kk]:
                    
                    if self.verbose:
                        print(
                            f"'{d}' is linked to '{nl}' and is being remapped to '{mappings.default[kk]}'"
                        )

                    # remap to default format
                    self.df[d] = self.df[d].map(
                        dict(
                            zip(
                                mappings.maps[kk][nl],
                                mappings.maps[kk][mappings.default[kk]],
                            )
                        )
                    )


      
    def test_notation(self):
        self.__to_drop__ = {}
        v = self.df

        # look through all linked columns of the dataframe
        for d, nl in self.notation_link:
            if self.verbose:
                print(f"column name {d} linked to {nl}")
            self.__to_drop__[d] = []

            # set up new table for output display
            table_data = []
            table_title = f"{d}"

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
                if self.verbose:
                    print(table.table)

            # if data is a subset of notation
            elif data.issubset(notation) and data != notation:
                if self.verbose:
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
                if self.verbose:
                    print(table.table)

            # if data is a superset of notation
            elif data.issuperset(notation) and data != notation:
                self.__to_drop__[d].extend(diff_data)
                if self.verbose:
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
                if self.verbose:
                    print(table.table)

            # if symmetric difference is not empty and the length of differences are ==
            elif data.symmetric_difference(notation) != set() and len(
                diff_notation
            ) == len(diff_data):
                self.__to_drop__[d].extend(diff_data)
                if self.verbose:
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
                if self.verbose:
                    print(table.table)

            # if the left and right differences are != and diff_notation not empty
            elif len(diff_notation) != len(diff_data) and diff_notation != set():
                self.__to_drop__[d].extend(diff_data)
                if self.verbose:
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
                if self.verbose:
                    print(table.table)

            else:
                if self.verbose:
                    print("**** UNKNOWN ISSUE in notation link")
            if self.verbose:
                print("")           
                
                
                
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    