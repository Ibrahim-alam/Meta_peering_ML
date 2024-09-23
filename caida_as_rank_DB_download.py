#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May 27 23:51:02 2024

@author: papuska
"""

import urllib.request, json 
import numpy as np
import pandas as pd

#%%
ISP_peering_info = pd.read_csv('20240101.as-rel_mod.txt', sep = '|', header= None)
ISP_list1 = list(ISP_peering_info.iloc[:,0])
ISP_list2 = list(ISP_peering_info.iloc[:,1])
ISPs = list(set(ISP_list1 + ISP_list2))
ISPs.sort()
CAIDA_info_dict = {}

#%%
for i in range(len(ISPs)):
    with urllib.request.urlopen("https://api.asrank.caida.org/v2/restful/asns/"+str(ISPs[i])) as url:
        data = json.load(url)
        temp1 = data['data']['asn']
        CAIDA_info_dict |= {str(ISPs[i]):temp1}


#%% Saving in JSON format

with open("CAIDA_data.json", "w") as outfile: 
    json.dump(CAIDA_info_dict, outfile)
    
#%% If you need to save in pickle format

# import pickle as pkl   

# with open('CAIDA_data.pkl', 'wb') as fp:
#     pkl.dump(CAIDA_info_dict , fp)
    
# #%% Loading Pickle files

# with open('CAIDA_data.pkl', 'rb') as input_file:
#     CAIDA_data = pkl.load(input_file)
    


#%% Some debugging (commented out)

# total_asn = 116377
# with urllib.request.urlopen("http://api.asrank.caida.org/v2/restful/asns/?first=1&offset="+str(116377)) as url:
#     data = json.load(url)

# with urllib.request.urlopen("http://api.asrank.caida.org/v2/restful/asns/?first="+str(total_asn)) as url:
#     data2 = json.load(url)
