# Meta_peering_ML
Use of ML on publicly available data of ASNs to predict if two ASNs should peer or not.

## Step 1: Getting PeeringDB features
Download PeeringDB dump file from PeeringDB - maintained by CAIDA (https://publicdata.caida.org/datasets/peeringdb/). Any file in the version 2.0 format (After January 2017) should work.

## Step 2: Getting CAIDA features
Run the CAIDA_as_rank_DB file to generate json file that has the ASN features of all ASNs available from CAIDA.
