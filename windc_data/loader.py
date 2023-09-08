# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 07:50:35 2022

@author: Mitch
"""

import pandas as pd
import os,json

from . import parse as pr

datasets = {
    "bea_gsp":pr.BeaGsp,
    #"bea_pce":pr.BeaPce,
    #"bea_supply_det":pr.BeaSupplyDet,
    #"bea_supply" : pr.BeaSupply,
    #"bea_use_det" : pr.BeaUseDet,
    #"bea_use" : pr.BeaUse,
    #"census_sgf" : pr.CensusSGF,
    #"cfs" : pr.CFS,
    #"eia_crude_price" : pr.EiaCrude,
    #"eia_emissions" : pr.EiaEmissions,
    #"eia_heatrate" : pr.EiaHeatrate,
    #"eia_seds" : pr.EiaSeds,
    #"nass" : pr.Nass,
    #"state_exim" : pr.StateExIm,
    }



class WindcEnvironment:
    
    """
    The primary container for all the data loading and output. 
    
    Inputs:
    
        data_dir - The parent directory of the data. This will look something like
                   r"C:\path\to\data\windc_3_0_0"
                   
        load - A dictionary telling what datasets to load
                key   - An identifier string
                value - A class instantiator 
                   
        years - Defaults to (1997,2020). Data outside of this range will be dropped.
                   
        verbose - Optional argument, defaults to True. If false will hide all the 
                  printed output.
    """


    def __init__(self,
                 data_dir,
                 load = datasets,
                 years=(1997,2020),
                 verbose = True,
                 json_path = "data_information.json"):
        
        
        self.data = {}
        
        with open(os.path.join(data_dir,json_path),"r") as d:
            self.json = json.load(d)
        
        self.years = years
        
        self.gams_maps = pr.GamsMaps(data_dir)
        
        for key,t in load.items():
            data_info = {}
            if key in self.json:
                data_info = self.json[key]
            
            self.data[key] = t(data_dir,
                               self.gams_maps,
                               years = years, 
                               verbose = verbose,
                               data_info = data_info
                               )


        self.gdx_data = {}
        gdx_sets = self._gdx_sets()
        for key,d in gdx_sets.items():
            self.gdx_data[key] = d#['elements']

        for key in self.data:
            gdx_data  = self.data[key]._build_gdx_dict()
            for key,d in gdx_data.items():
                self.gdx_data[key] = d#['elements']


    def __iter__(self):
        return iter(self.gdx_data)



    def __getitem__(self,key):
        
        return self.gdx_data[key]['elements']
        
        #return self.data[key].get_df()


    def keys(self):
        return self.gdx_data.keys()

    def to_csv(self,output_dir):
        """
        Output all currently loaded datasets as CSV files. The file names will
        be the key from the initialized load variable. 
        
        Parameters
        ----------
        output_dir : String 
            The directory where the CSV files will live.
        """
        
        if not os.path.exists(os.path.join(output_dir,"set")):
            os.mkdir(os.path.join(output_dir,"set"))

        if not os.path.exists(os.path.join(output_dir,"parameter")):
            os.mkdir(os.path.join(output_dir,"parameter"))

        for key in self.gdx_data:
            df = self.gdx_data[key]['elements']
            dir = self.gdx_data[key]['type']
            df.to_csv(os.path.join(output_dir,dir,key+".csv"),index=False)


    def _gdx_sets(self):
        gdx_dict = {}
        
        
        gdx_dict['r'] = {"type":"set",
                         "elements":self.gams_maps['states'].query("abbv!='US'"),
                         "text":"States in WiNDC Database"}
        
        gdx_dict['sr'] = {"type":"set",
                         "elements":self.gams_maps['states'],
                         "text":"States + US in WiNDC Database"}
        
        gdx_dict['i'] = {"type":"set",
                         "elements":self.gams_maps['bea_all'].query("category == 'goods'")[['windc_label','description_code']],
                         "text":"BEA Goods and sectors categories"
                         }
        
        
        gdx_dict['fd'] = {"type":"set",
                         "elements":self.gams_maps['bea_all'].query("category == 'finaldemand'")[['windc_label','description_code']],
                         "text":"BEA Final demand categories"
                         }
        
        gdx_dict['ts'] = {"type":"set",
                         "elements":self.gams_maps['bea_all'].query("category == 'taxessubsidies'")[['windc_label','description_code']],
                         "text":"BEA Taxes and subsidies categories"
                         }
        
        gdx_dict['va'] = {"type":"set",
                         "elements":self.gams_maps['bea_all'].query("category == 'valueadded'")[['windc_label','description_code']],
                         "text":"BEA Value added categories"
                         }
      
        
        gdx_dict['i_det'] = {"type":"set",
                         "elements":self.gams_maps['bea_all_det'].query("category == 'goods'")[['windc_label','description_code']],
                         "text":"Detailed BEA Goods and sectors categories (2007 and 2012 only)"}
        
        gdx_dict['sector_map'] = {"type":"set",
                         "elements":self.gams_maps['bea_all_det'].query("category == 'goods'")[['windc_label','windc_aggr_label',"description_code"]],
                         "text":"Mapping between detailed and aggregated BEA sectors"}
        
        gdx_dict['seds_src'] = {"type":"set",
                         "elements":self.gams_maps['eia_gen'][['windc_label','description']],
                         "text":"Energy Technologies in EIA SEDS Data"}
        
        
        gdx_dict['yr'] = {'type':'set',
                          'elements':pd.DataFrame(zip(list(range(self.years[0],self.years[1]+1)),list(range(self.years[0],self.years[1]+1))),columns = ['yr',"description"]),
                          'text':"Years in WiNDC Database"}
        
        gdx_dict["version"] = {
            "type": "set",
            "elements": pd.DataFrame(data = self.json['version'],index = [0]),
            "text": "WiNDC data version number",
        }
        
        return gdx_dict


    def to_gdx(self,output_dir,output_name = "windc.gdx"):
        import gams.transfer as gt
        
        gdx_container = gt.Container()
        
        gdx_sets = self._gdx_sets()
        
        for key,d in gdx_sets.items():
        #    for key,d in [(k,d) for (k,d) in gdx_dict.items() if d['type'] == 'set']:
            if key not in gdx_container.data:
                num_domain = 1
                if type(d['elements']) == type(pd.DataFrame()):
                    num_domain = len(d['elements'].columns)-1
                gdx_container.addSet(key,["*"]*num_domain,records = d['elements'],description = d['text'])
        
        
        for key in self.data:
            self.data[key].build_gdx(gdx_container)
            
        gdx_container.write(os.path.join(output_dir,output_name))



    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    