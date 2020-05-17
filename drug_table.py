"""Drug Table.

This module works in order to get drug information and insert them into DISNET drugslayer database.

It fills 5 tables: drug, ATC_code, code, has_code and synonymous.

From the chEMBL python client the data for drug, ATC_code and synonymous are extracted.

The data for code and has_code are got from UNICHEM python client to get the cross reference between chEMBL and DrugBank .


"""

import requests
import conection_DISNET_drugslayer
from get_list import get_list

cursor = conection_DISNET_drugslayer.cursor

# Get drug table
drug_table = get_list("select * from drug")

# get column length
column_length=len(drug_table[0])

# Get the primary keys (pk) from drug table 
drug_pk_table = get_list("select drug_id from drug")
drug_pk_table=list(*zip(*drug_pk_table))

# Get ATC_code table
ATC_table = get_list("select * from ATC_code")

# Get synonymous table
synonymous_table = get_list("select * from synonymous")

# Get columns names
columns_names = get_list("""SELECT COLUMN_NAME FROM information_schema.COLUMNS 
                        WHERE TABLE_SCHEMA LIKE 'disnet_drugslayer' AND TABLE_NAME = 'drug' """)



#Initialize auxiliaries and lists

# These variables will keep the intersection between old data and new data
intersection_drugs=[] 
intersection_ATC_code = [] 
intersection_synonymous = [] 

# Counts for keeping the quantity of data that are going to be inserted
count_drug=0
count_ATC=0
count_code=0
count_synonymous=0

# These Lists are going to keep the pk of the data that will be inserted
drug_list=[]
ATC_code_list=[]
code_list=[]
has_code_list=[]
synonymous_list=[]

# These Lists are going to keep tuples with the data which will be inserted in the tables
new_drug_list=[]
new_ATC_code_list=[]
new_code_list=[]
new_synonymous_list=[]

# Counts for get the quantity of INSERT, UPDATE and DELETE
n_ins_drug = 0
n_ins_atc = 0
n_ins_code = 0
n_ins_synonymous = 0
n_upd_drug = 0
n_del_drug = 0
n_del_atc = 0
n_del_synonymous = 0
n_same_drug = 0
n_same_atc = 0
n_same_synonymous = 0

# Resource id
# DB --> DrugBank
DB_RESOURCEID = 95

# Entity id
DRUG_ENTITYID = 2

# Source from data in this file were extracted
SOURCE = "CHEMBL"

# Get the source_id of SOURCE from the table "source"
source_id=int(get_list("SELECT source_id from source where name = '%s'" % SOURCE)[0][0])


# Get the info from chEMBL Python Client
from chembl_webresource_client.new_client import new_client
molecule = new_client.molecule
approved_drugs = molecule.filter(max_phase=4) # max_phase = 4, means the approved drugs

for i in approved_drugs:
    drug_id = i['molecule_chembl_id']
    pref_name = i['pref_name']
    molecular_type = i['molecule_type']
    synonym = i['molecule_synonyms']

    # There are some Approved drugs without molecule structures,
    # However, it that drug does not have them, those features will be None
    if not i['molecule_structures'] == None:
        smiles = i['molecule_structures']['canonical_smiles']
        inchi_key = i['molecule_structures']['standard_inchi_key']
    else:
        if not drug_id in new_drug_list:
            smiles = None
            inchi_key = None      

    drug=(drug_id,source_id,pref_name,molecular_type,smiles,inchi_key)
    

    # INSERT: primary key (pk) that is not in the previous version
    # Sometimes there are repeat pk in the new data, 
    # in order to avoid duplicate primary key of the tables,
    # it keeps all the pk in a list.
    # Other list keeps tuples with the data which will be inserted in the tables, 
    # They will be inserted each 500 rows.
    # This method is faster than handle the exception and insert data one per one. 
    
    # UPDATE: primary key is that is in the previous version of the table
    # If all the data is the same it is repeat data
    # If the data is different is an update

    if not drug_id in drug_pk_table: 
        if not drug_id in new_drug_list: 
            new_drug_list.append(drug_id) 
            drug_list.append(drug) # Provisional list with the new complete data
            count_drug+=1
            n_ins_drug+=1

        # For each new drug id get the DrugBank id
        if not (drug_id,DRUGBANK_RESOURCE_ID,DRUG_ENTITY_ID) in new_code_list:  
            # Get the DrugBank code
            from chembl_webresource_client.unichem import unichem_client as unichem
            response=unichem.get(drug_id,1,2) #1 --> CHEMBL, 2 --> DrugBank
            # With the DrugBank code will be fill the table code and has_code
            if response:
                db_code = response[0]['src_compound_id']
                code=(db_code,DB_RESOURCEID,DRUG_ENTITYID)
                has_code=(drug_id,db_code,DB_RESOURCEID,DRUG_ENTITYID)
                chembl_cr=(drug_id,DB_RESOURCEID,DRUG_ENTITYID)
                new_code_list.append(chembl_cr)
                code_list.append(code)
                has_code_list.append(has_code)
                count_code+=1 
                n_ins_code+=1  

    #  Get the atc code    
    if 'atc_classifications'in i:
        for j in i['atc_classifications']:
            ATC_code=(drug_id,j,source_id)
            if not ATC_code in ATC_table:
                if not ATC_code in new_ATC_code_list: 
                    new_ATC_code_list.append(ATC_code) 
                    ATC_code_list.append(ATC_code) 
                    count_ATC+=1
                    n_ins_atc+=1
            else:
                intersection_ATC_code.append(ATC_code) # Add the pk that is in the previous and the actual version
                n_same_atc+=1
    # Get the synonym name of each drug    
    for j in synonym:
        synonymous=(drug_id,source_id,j['molecule_synonym'])
        if not synonymous in synonymous_table: 
            if not synonymous in new_synonymous_list: 
                new_synonymous_list.append(synonymous) 
                synonymous_list.append(synonymous) 
                count_synonymous+=1
                n_ins_synonymous += 1
        else:
            intersection_synonymous.append(synonymous) # Add the pk that is in the previous and the actual version
            n_same_synonymous+=1
    # UPDATE drug table
    else:
        intersection_drugs.append(drug_id) # Add the pk that is in the previous and the actual version 
        n_same_drug += 1
        for row in drug_table: 
            PV_drug_id = row[0] 
            if drug_id == row: 
                for column in range(1,column_length): # to loop all -not pk- the columns of the drug's table
                    if row[column] != drug[column]: 
                        drug_update_values = (columns_names[column][0],drug[column], drug_id) 
                        cursor.execute("UPDATE drug SET %s = '%s' where drug_id = '%s'" % drug_update_values)
                        n_upd_drug +=1
                        
                  
    # Insert the Data each 500 rows in each list
    if count_drug==500:
        cursor.executemany("insert into drug values(%s,%s,%s,%s,%s,%s)",drug_list)
        drug_list=[]
        count_drug=0
    if count_code==500:
        cursor.executemany("insert into code values(%s,%s,%s)",code_list)
        cursor.executemany("insert into has_code values(%s,%s,%s,%s)",has_code_list)
        code_list=[]
        has_code_list=[]
        count_code=0
    if count_ATC == 500:
        cursor.executemany("insert into ATC_code values(%s,%s,%s)",ATC_code_list)
        ATC_code_list=[]
        count_ATC=0
    if count_synonymous==500:
        cursor.executemany("insert into synonymous values(%s,%s,%s)",synonymous_list)
        synonymous_list=[]
        count_synonymous=0


# Insert the remaining data in the lists
cursor.executemany("insert into drug values(%s,%s,%s,%s,%s,%s)",drug_list)
cursor.executemany("insert into ATC_code values(%s,%s,%s)",ATC_code_list)
cursor.executemany("insert into code values(%s,%s,%s)",code_list)
cursor.executemany("insert into has_code values(%s,%s,%s,%s)",has_code_list)
cursor.executemany("insert into synonymous values(%s,%s,%s)",synonymous_list)

# DELETE:
# primary key that is in the previous version of the table and not in the new one -intersection list-


for PV_drug_id in drug_pk_table: # Loop the pk of the drug's table
    if not PV_drug_id in intersection_drugs: # The PK of the Previous version of the table is not in the intersection_drugs list ?
        cursor.execute("DELETE FROM drug WHERE drug_id = '%s'" % PV_drug_id)
        n_del_drug += 1

for row in ATC_table: # Loop the pk of the ATC_code's table
    PV_ATC_pk =(row[0],row[1],source_id) # The PK of the Previous version of the table is not in the intersection_drugs list ?
    if not PV_ATC_pk in intersection_ATC_code:
        cursor.execute("DELETE FROM  ATC_code WHERE drug_id = '%s'  and source_id = '%s' and ATC_code_id = '%s'" % PV_ATC_pk)
        n_del_atc+=1

for row in synonymous_table: # Loop the pk of the synonymous's table
    PV_synonymous_pk =(row[0],source_id,row[2]) # The PK of the Previous version of the table is not in the intersection_drugs list ?
    if not PV_synonymous_pk in intersection_synonymous:
        cursor.execute("DELETE FROM synonymous WHERE drug_id = '%s' and source_id = '%s' and synonymous_name = '%s'" % PV_synonymous_pk)
        n_del_synonymous+=1


print("Number of Inserted in the drug table: ", n_ins_drug, "\nNumber of Updates in the drug table: ", n_upd_drug, "\nNumber of Deletes in the drug table:  ",n_del_drug, "\nNumber of repeat PK in the drug table: ",n_same_drug)

print("Number of Inserted in the code and has_code table: ", n_ins_code)

print("Number of Inserted in the ATC_code table: ", n_ins_atc, "\nNumber of Deletes in the ATC_code table:  ",n_del_atc,"\nNumber of no changes in the ATC_code table: ",n_same_atc)

print("Number of Inserted in the synonymous table: ", n_ins_synonymous, "\nNumber of Deletes in the synonymous table:  ",n_del_synonymous, "\nNumber of no changes in the synonymous table: ",n_same_synonymous)