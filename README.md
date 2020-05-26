# DISNET-DRUGSLAYER
Scripts for creation DISNET drug's layer

# Requirements
1. Have installed a MySQL connector/python 
The instalation guide is in https://dev.mysql.com/doc/connector-python/en/connector-python-installation.html

2. Install the chEMBL Python Client  
pip install chembl_webresource_client

3. Have an ApiKey for UMLS Metathesaurus --> write it in the get_umls script
https://uts.nlm.nih.gov/license.html

4. Wirte the parameter of the conection to the database in the script conection_DISNET_drugslayer.py

# Quick start For the first time use

1. Run the script main_first_time_use.py
<ul> 
  <li>It will Create the tables</li>
  <li>It will insert some important data in the source, entity and code_reference tables</li>
  <li>It fill all the tables</li>
</ul>

# Update Tables

1. Run the script main_update
<ul>
  <li>It fill all the tables</li>
</ul>
