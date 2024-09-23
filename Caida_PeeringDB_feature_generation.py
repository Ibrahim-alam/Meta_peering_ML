#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 26 13:59:18 2024

@author: papuska
"""

import urllib.request, json 
import numpy as np
import pandas as pd
import pickle as pkl 
import time


#%% Functions and dictionaries

info_ratio = {
	"":-10, "Not Disclosed": -10,
	"Heavy Outbound": -2, "Mostly Outbound": -1,
	"Balanced": 0, "Mostly Inbound": 1,
	"Heavy Inbound": 2
    }

policy_general = {
    "": -5, "No": -1, "Restrictive": 0,
	"Selective": 1, "Open": 2
    }

info_types = {
    "NSP": "transit", "Content": "content", "Non-Profit": "non_profit", 
    "Cable/DSL/ISP": "access", "": "unknown", "Enterprise": "enterprise",
    "Educational/Research": "education", "Route Server": "route_server",
    "Not Disclosed": "unknown", 'Government' : 'government', 
    'Network Services' : 'network', 'Route Collector': 'route_collector'
    }


info_traffic = {
    "": -20, 
    "0-20Mbps": 0, "20-100Mbps": 1,
    "100-1000Mbps": 2, "1-5Gbps": 3, "5-10Gbps": 4,
    "10-20Gbps": 5, "20-50Gbps": 6, "50-100Gbps": 7,
    "100-200Gbps": 8, "200-300Gbps": 9, "300-500Gbps": 10,
    "500-1000Gbps": 11, "1 Tbps+": 12, "1-5Tbps": 13,
    "5-10Tbps": 14, "10-20Tbps": 15, "20-50Tbps": 16,
    "50-100Tbps": 17, "100+Tbps": 18,
    "0-20 Mbps": 0, 
    "20-100 Mbps": 1, "100-1000 Mbps": 2,
    "1-5 Gbps": 3, "5-10 Gbps": 4, "10-20 Gbps": 5,
    "20-50 Gbps": 6, "50-100 Gbps": 7, "100-200 Gbps": 8, 
    "200-300 Gbps": 9, "300-500 Gbps": 10,"500-1000 Gbps": 11,
    "1 Tbps+": 12, "1-5 Tbps": 13, "5-10 Tbps": 14,
    "10-20 Tbps": 15,"20-50 Tbps": 16,
    "50-100 Tbps": 17, "100 Tbps+": 18
    }



policy_locations = {
    "": -10, "Not Required": 0, 
	"Preferred": 1,  "Required - US": 2, 
	"Required - EU": 3, "Required - International": 4
    }


info_scope = {
    "":"nD", "Regional": "R", "North America": "NAm",
    "Asia Pacific": "AP", "Europe": "E", "South America": "SAm",
    "Africa": "Af", "Australia": "Au","Middle East": "ME",
    "Global": "G", 'Not Disclosed': 'nD'
    }

policy_contracts = {
    "": -5, "Not Required": 0,
 	"Private Only": 1, "Required": 2
}


#%% NOT NEEDED NOW
# policy_ratio = {
#     "FALSE": 0, "True": 1
# }

# info_unicast = {
#     "FALSE": 0, "True": 1
# }

# allow_ixp_update = {
#     "FALSE": 0, "True": 1
# }

# info_multicast = {
#     "FALSE": 0, "True": 1
# }


# policy_general_combo = {
#     "Open Open": 14,
#     "Open Selective": 13,
#     "Selective Selective": 12,
#     "Open Restrictive": 11,
#     "Restrictive Selective": 10,
#     "Restrictive Restrictive": 9,
#     " Open": 8,
#     "No Open": 7,
#     " Selective": 6,
#     "No Selective": 5,
#     " Restrictive": 4,
#     "No Restrictive": 3,
#     " ": 2,
#     " No": 1,
#     "No No": 0
# }





# info_type_abv = {
#     "transit": "T",
#     "content": "C",
#     "access": "A"
# }

# info_type_score = {
#     "*":0,
#     "T":2,
#     "C":3,
#     "A":4
# }

# info_ratio_abv = {
# 	"Heavy Inbound": "HO",
# 	"Mostly Inbound": "MO",
# 	"Balanced": "B",
# 	"Mostly Outbound": "MI",
# 	"Heavy Outbound": "MO"
# }



# info_scope_score = {
#     "*",
#     "R",
#     "NA",
#     "AP",
#     "E",
#     "SA",
#     "Af",
#     "Au",
#     "ME",
#     "G"
# }


#%% Reading information on ASNs from CAIDA

start_time = time.time()

with open('CAIDA_data.pkl', 'rb') as input_file:
    CAIDA_data = pkl.load(input_file)

keys = list(CAIDA_data.keys())
column_names1 = sorted(list(CAIDA_data[keys[0]]))
remove_columns = [12, 11, 8, 7, 6, 3]

for i in range(len(remove_columns)):
    column_names1.remove(column_names1[remove_columns[i]])

CAIDA_list = []
for i in range(len(keys)):
    temp1 = []
    temp2 = CAIDA_data[keys[i]]
    if temp2 == None:
        continue
    for j in range(len(column_names1)):
        if column_names1[j] == 'asn':
            temp3 = temp2[column_names1[j]]
            temp1.append(int(temp3))
        elif column_names1[j] == 'asnDegree':
            temp3 = list(temp2['asnDegree'].values())
            for k in temp3:
                temp1.append(k)
        elif column_names1[j] == 'cone':
            temp3 = list(temp2['cone'].values())
            for k in temp3:
                temp1.append(k)
        elif column_names1[j] == 'country':
            temp3 = list(temp2['country'].values())
            for k in temp3:
                temp1.append(k)
        elif column_names1[j] == 'organization':
            if temp2['organization'] == None:
                temp1.append("No_name")
            else:
                temp3 = list(temp2['organization'].values())
                for k in temp3:
                    temp1.append(k)
        else:
            temp1.append(temp2[column_names1[j]])
    CAIDA_list.append(temp1)


df_columns = ['asn', 'total','customer','peer','provider','asnName','NumberASNs',
              'NumberPrefix', 'NumberAddrs','Country','Org','Rank']
CAIDA_data_df = pd.DataFrame(CAIDA_list, columns= df_columns)
CAIDA_data_df = CAIDA_data_df.drop(columns = ['asnName','Country','Org'])

CAIDA_asns = list(CAIDA_data_df['asn'])



#%% Reading information on ASNs from PeeringDB

f1 = open('peeringdb_2_dump_2024_06_01.json') # 'peeringdb_2_dump_2021_03_30.json'
x= json.load(f1)
net=x["net"]["data"]
column_names = list(net[0].keys())
net_df = pd.DataFrame(net)
PeeringDB_df = net_df.drop(columns = ['aka', 'name', 'name_long', 'status', 'created', 
                                'looking_glass', 'netfac_updated', 'netixlan_updated',
                                'notes', 'updated', 'policy_url', 'poc_updated' , 'website', 
                                'route_server', 'irr_as_set','social_media','status_dashboard',
                                'rir_status_updated','netfac_updated', 'poc_updated','info_type',
                                'irr_as_set','rir_status', 'info_never_via_route_servers'])

PeeringDB_asns = list(PeeringDB_df['asn'])


            
#%% Reading peering status of ASN pairs and Finding common ASNs

ISP_peering_info = pd.read_csv('20240601.as-rel_mod.txt', sep = '|', header= None) # '20210301.as-rel.txt'
peering_asn1 = ISP_peering_info[0]
peering_asn2 = ISP_peering_info[1]
peering_asn = sorted(list(set(peering_asn1) | set(peering_asn2)))


common_asn = sorted(list(set(PeeringDB_asns) & set(CAIDA_asns)))
columns_caida = CAIDA_data_df.columns
columns_peeringDB = net_df.columns
valid_asn = sorted(list(set(peering_asn) & set(common_asn)))


#%% Assigning features to ASNs common to all platform

ASN_features_list = []
for i in range(len(valid_asn)):
    asn_val = valid_asn[i]
    temp1 = CAIDA_data_df[(CAIDA_data_df['asn'] == asn_val)]
    temp2 = PeeringDB_df[(PeeringDB_df['asn'] == asn_val)]
    temp1 = temp1.drop(columns = ['asn']).reset_index(drop=True)
    temp2 = temp2.drop(columns = ['asn','id']).reset_index(drop=True) #'org_id']).reset_index(drop=True)
    temp3 = pd.concat([temp1, temp2], axis = 1)
    
    columns_common = list(temp3.columns)
    
    temp_list = [asn_val]
    for j in range(len(columns_common)):
        if columns_common[j] == 'info_ratio':
            temp4 = temp3['info_ratio'][0]
            temp5 = info_ratio[temp4]
            temp_list.append(temp5)
        elif columns_common[j] == 'policy_general':
            temp4 = temp3['policy_general'][0]
            temp5 = policy_general[temp4]
            temp_list.append(temp5)
        elif columns_common[j] == 'info_types':
            if len(temp3['info_types'][0]) == 0:
                temp4 =""
            else:
                temp4 = temp3['info_types'][0][0]
            temp5 = info_types[temp4]
            temp_list.append(temp5)
        elif columns_common[j] == 'info_traffic':
            temp4 = temp3['info_traffic'][0]
            temp5 = info_traffic[temp4]
            temp_list.append(temp5)
        elif columns_common[j] == 'policy_locations':
            temp4 = temp3['policy_locations'][0]
            temp5 = policy_locations[temp4]
            temp_list.append(temp5)
        elif columns_common[j] == 'info_scope':
            temp4 = temp3['info_scope'][0]
            temp5 = info_scope[temp4]
            temp_list.append(temp5)
        elif columns_common[j] == 'policy_contracts':
            temp4 = temp3['policy_contracts'][0]
            temp5 = policy_contracts[temp4]
            temp_list.append(temp5)
        else:
            temp_list.append(temp3.iloc[0][j])
    ASN_features_list.append(temp_list)

ASN_feature_columns = ['asn']+columns_common

#%%
ASN_feature_DF = pd.DataFrame(ASN_features_list, columns = ASN_feature_columns)
ASN_feature_DF.to_csv('ASN_with_features.csv', index=False)

end_time = time.time()
print('Time to create the features: '+f'{end_time - start_time:.2f}'+' sec')  
            




