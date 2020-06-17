""" The file CTD_chemicals_diseases contains the relationship between a chemical to a disease, however, only drugs must be chosen.
The identifier of the drugs in CTD is the MeSH code.
In order to do it, the file CTD_chemicals is used to get the DrugBank id of the drugs, and link them with the MeSh code of the chemical
Then the drugs in the file CTD_chemicals_diseases will be filtered """

import requests
import gzip
import csv
import itertools
import re
import get_umls
import conection_DISNET_drugslayer
from get_list import get_list
cursor = conection_DISNET_drugslayer.cursor


# links where the data are, and their names
url_chemical_diseases = 'http://ctdbase.org/reports/CTD_chemicals_diseases.tsv.gz'
file_name_chemical_diseases='CTD_chemicals_diseases.tsv.gz'
url_chemical="http://ctdbase.org/reports/CTD_chemicals.tsv.gz"
file_name_chemical="CTD_chemicals.tsv.gz"

def request_url(url,file_name):
    myfile = requests.get(url)
    open(file_name, 'wb').write(myfile.content)

# Download the file CTD_chemicals_diseases and CTD_chemicals file, this file contains the drugs and the related diseases 
request_url(url_chemical_diseases,file_name_chemical_diseases)
request_url(url_chemical,file_name_chemical)

# Get the DrugBank codes of each drug from the has_code table, keep them in a diccionary. 
# The  DrugBank id as a key and the chEMBL id as a value 
drugbank_chembl={}
cross_references_list=get_list("SELECT * FROM has_code where entity_id = 2 and resource_id = 95 ")
for i in cross_references_list:
    chembl_code = i[0]
    drugbank_code = i[1]
    drugbank_chembl[drugbank_code]=chembl_code

# Get tables disease and drug_disease
disease_table=get_list("select * from disease")
drug_disease_table=get_list("select *from drug_disease")

# Get the primary keys (pk) from the disease and drug disease tables
disease_pk_table=get_list("select disease_id from disease")
disease_pk_table=list(*zip(*disease_pk_table))
drug_disease_pk_table=get_list("select disease_id, drug_id from drug_disease")


# The identifier of the drugs in CTD is the MeSH code.
# A dicctionary is created in order to keep the relation between a MeSH code with a DrugBank id
meshdrug_drugbank={}
with gzip.open('CTD_chemicals.tsv.gz','rb') as f1:
    for line in itertools.islice(f1, 30, None): # The data start at line 30
        lines=(line.decode().strip().split('\t'))
        if len(lines)>8:
            if re.match(r'^MESH:\w+',lines[1]):
                meshcodes=re.split('MESH:',lines[1])
                meshdrug=meshcodes[1]            
                drugbank_splitcodes=lines[8].split("|")
                drugbank_id=drugbank_splitcodes[0]
                meshdrug_drugbank[meshdrug]=drugbank_id

# Initialize auxiliaries
disease_list=[]
new_disease_list=[]
count=0

drug_disease_list=[]
new_drug_disease=[]

intersection_disease = []
intersection_drug_disease  = []




# Source from data in this file were extracted
SOURCE="CTD"
cursor.execute("SELECT source_id from source where name = '%s'" % SOURCE)
source=cursor.fetchall()
source_id=int(source[0][0])
l=0
with gzip.open(file_name_chemical_diseases,'rb') as f2:
    for line in itertools.islice(f2, 6799914, 7000270): # The data start at line 30
        l+=1
        print(l)
        lines=(line.decode("UTF-8").strip().split('\t'))
        inference_score = lines[7]
        direct_evidence = lines[5]
        chemical_id = lines[1]
        disease_old_id = lines[4] #The disease id OMIM or MeSH
        disease_name = 	lines[3]
        
        # some drug-disease association has information about Direct Evidence	      
        if inference_score:
            score=float(inference_score)
        else:
            if direct_evidence == "therapeutic":
                score=1000
            if direct_evidence == "marker/mechanism":
                score=2000
        
        if chemical_id in meshdrug_drugbank:
            if (re.match(r'^MESH:\w+',disease_old_id)):
                meshcodes=re.split('MESH:',disease_old_id)
                disease_id=meshcodes[1]
                reference_id= 75
                
            if (re.match(r'^OMIM:\w+',disease_old_id)):
                omimcodes=re.split('OMIM:',disease_old_id)
                disease_id=omimcodes[1]
                reference_id= 72

            # If the chemical_id(MeSH) has a DrugBank_id  is because is a drug
            drugbank_id=meshdrug_drugbank[chemical_id]

            # Check if the drug is in our database
            if drugbank_id in drugbank_chembl:
                drug_id=drugbank_chembl[drugbank_id]
                disease=(reference_id, disease_id, source_id, disease_name)
                drug_disease_pk = (disease_id, drug_id)
                drug_disease=(disease_id, drug_id, source_id, score)

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
                
                if not disease_id in disease_pk_table:
                    if not disease_id in new_disease_list: 
                        new_disease_list.append(disease_id) 
                        disease_list.append(disease) 
                
                else: 
                    intersection_disease.append(disease_id) # Add the pk that is in the previous and the actual version
                    for rows in disease_table:
                        PV_disease_id=rows[1]
                        PV_disease_name = rows[3]
                        if disease_id == PV_disease_id:
                            if PV_disease_name != disease_name:
                                disease_update_values = (disease_name, disease_id)
                                cursor.execute("UPDATE disease SET disease_name = '%s' where disease_id = '%s'" % disease_update_values)
                
                # Drug - Disease
                if not drug_disease_pk in drug_disease_pk_table: 
                    if not  drug_disease_pk in new_drug_disease: 
                        new_drug_disease.append(drug_disease_pk) 
                        drug_disease_list.append(drug_disease) 
                        count+=1
                        print(count)

                else:
                    intersection_drug_disease.append(drug_disease_pk) # Add the pk that is in the previous and the actual version
                    for rows in drug_disease_table:
                        if disease_id == rows[0] and drug_id == rows[1]:
                            if score > rows[3]:
                                drug_disease_update_values = (score, disease_id, drug_id)
                                cursor.execute("UPDATE drug_disease SET score = '%s' where disease_id = '%s' and drug_id = '%s' " % drug_disease_update_values)
                if count == 500:
                    cursor.executemany("insert into disease values(%s,%s,%s,%s)",disease_list)
                    disease_list=[]
                    cursor.executemany("insert into drug_disease values(%s,%s,%s,%s)",drug_disease_list)
                    drug_disease_list=[]
                    count=0

cursor.executemany("insert into disease values(%s,%s,%s,%s)",disease_list)
cursor.executemany("insert into drug_disease values(%s,%s,%s,%s)",drug_disease_list)



# Delete data
# if old data =! the new data
# for PV_disease_id in disease_pk_table:
#     if not PV_disease_id in intersection_disease:
#         cursor.execute("DELETE FROM disease WHERE disease_id = '%s'" % PV_disease_id)

# for PV_drug_disease in drug_disease_pk_table:
#     PV_drug_disease_pk = (PV_drug_disease[0],PV_drug_disease[1])
#     if not PV_drug_disease_pk in intersection_drug_disease:
#         cursor.execute("DELETE FROM drug_disease WHERE disease_id = '%s' and drug_id = '%s'" % PV_drug_disease_pk)



  