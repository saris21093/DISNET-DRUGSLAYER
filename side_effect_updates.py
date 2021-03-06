""" 
The function of this module works in order to get side effect information and insert them into DISNET drugslayer database.
this module will fill 2 tables: phenotype_effect and drug_phenotype_effect.

From the chEMBL python client the data for drug, ATC_code and synonymous will be extracted. In adittion, the data for code and has_code will be extracted with an API from EBI to cross_reference.
The sider Database has a different identifiers for the drugs --> STITCH,therefore this needs to be changed to chEMBL. However they do not have any other identifier in the relationship between chemicals and disease file. So the way to solve this problem is to download a file called drug_atc, this allows to map the STITCH id to ATC code, that already is kept in the database."""
import requests
import gzip
import csv
import itertools
import re
import conection_DISNET_drugslayer
from get_list import get_list
cursor = conection_DISNET_drugslayer.cursor


# Get tables phenotype_effect and drug_phenotype_effect
# pe --> phenotype effect

pe_table = get_list("select * from phenotype_effect where source_id = 2")
drug_pe_table = get_list("select * from drug_phenotype_effect where source_id =2")

# Get the primary keys (pk) of phenotype effect and drug- phenotype effect tables 

pe_pk_table = get_list("select phenotype_id, source_id from phenotype_effect where source_id = 2")
drug_pe_pk_table = get_list("select phenotype_id, drug_id, source_id from drug_phenotype_effect where source_id = 2")


# links where the data are, and their names
url_drug_atc = 'http://sideeffects.embl.de/media/download/drug_atc.tsv'
file_name_drug_atc='drug_atc.tsv'

url_side_effects = 'http://sideeffects.embl.de/media/download/meddra_freq.tsv.gz'
file_name_side_effects='meddra_freq.tsv.gz'

def download_file(url,file_name):
    """ This is a fuction to download the data, the params are the url of the file and the name. """
    myfile = requests.get(url)
    open(file_name, 'wb').write(myfile.content)

# Download the file drug_atc and meddra_freq file, this file contains the drugs and their side effects
download_file(url_drug_atc,file_name_drug_atc)
download_file(url_side_effects,file_name_side_effects)

# Create a diccionary in order to keep the information, ATC codes as a key, and the STITCH as values.
def atc_stitch_codes(file_atc):
    """ This function takes the drug_atc file in order to create a diccionary in order to keep the information, ATC codes as a key, and the STITCH as values."""
    atc_stitch={}
    with open(file_atc) as tsvfile:
        atc_codes = csv.reader(tsvfile, delimiter="\t")
        for line in atc_codes:
            atc=line[1]
            stitch=line[0]
            atc_stitch[atc]=stitch
    return atc_stitch

# Get the ATC_STITCH
atc_stitch_dic=atc_stitch_codes(file_name_drug_atc)

# Get the ATC codes with its correspond chEMBL id from DISNET database
ATC_code_table=get_list("select drug_id, ATC_code_id from ATC_code")

# iterate through the ATC_code_table in order to get a diccionary with STITCH id as keys and chEMBL id as values.
chembl_stitch_dic={}
for i in ATC_code_table:
    drug_id=i[0]
    atc_code=i[1]
    if atc_code in atc_stitch_dic:
        chembl_stitch_dic.setdefault(atc_stitch_dic[atc_code],drug_id)

# Initialize auxiliaries
# se --> side effect
se_list=[]
new_se=[]

drug_se_list=[]
new_drug_se=[]   

# These variables will keep the intersection between old data and new data
intersection_se=[]
intersection_drug_se=[]

# Source from data in this file were extracted
SOURCE = "SIDER"
cursor.execute("SELECT source_id from source where name = '%s'" % SOURCE)
source=cursor.fetchall()
source_id=int(source[0][0])

# Open the file with the relationship between drugs and side effects, called file_name_side_effects
# Unzip and read the file
with gzip.open(file_name_side_effects,'rb') as f:
    for lines in itertools.islice(f,0,None):
        line=(lines.decode().strip().split('\t'))
        stitch_code = line[0]
        concept_type = line[7]
        if concept_type =='PT': # Filter by PT, preferer term in the line 7 called MedDRA concept type
            if stitch_code in chembl_stitch_dic: #Filter if STITCH code is in the ch_st dictionary
                drug_id=chembl_stitch_dic[stitch_code]
                se_id = (line[8]) 
                frec_se=(float(line[6]))
                phenotype_type="SIDE EFFECT"
                se_name=line[9]
                se_name = se_name.replace("'", "\\'") # Avoid the problem with the "'" at the moment to insert the data to the Database we replace it by "\' "
               
                se_pk = (se_id, source_id) # Primary keys side effect --> phenotype_effect table
                se = (se_id,source_id,se_name) # side effect data
                drug_se_pk = (se_id,drug_id,source_id) # Primary keys drug_phenotype_effect
                drug_se =(se_id,drug_id,source_id,frec_se,phenotype_type) # drug side effect data
                
                # INSERT: primary key (pk) that is not in the previous version
                # Sometimes there are repeat pk in the new data, 
                # in order to avoid duplicate primary key of the tables,
                # it keeps all the pk in a list.
                # Other list keeps tuples with the data which will be inserted in the tables, 
                # This method is faster than handle the exception and insert data one per one. 

                # UPDATE: primary key is that is in the previous version of the table
                # If all the data is the same it is repeat data
                # If the data is different is an update

                if not se_pk in pe_pk_table:
                    if not se_pk in se_list:
                        se_list.append(se_pk)
                        new_se.append(se) 
                else:
                    intersection_se.append(se_pk) # Add the pk that is in the previous and the actual version
                    for row in pe_table:
                        PV_se_id = row[0]
                        PV_se_sourceid = row[1]
                        PV_se_name = row[2]
                        if se_id == PV_se_id and PV_se_sourceid == source_id:
                                if PV_se_name != se_name:
                                    se_update_values = (se_name,se_id,source_id )
                                    cursor.execute("UPDATE phenotype_effect SET phenotype_name = '%s' where phenotype_id = '%s' and source_id = '%s'" % se_update_values)

                # Drug-side effect 
                # INSERT: primary key (pk) that is not in the previous version
                # Sometimes there are repeat pk in the new data, in this case.
                # There are several drug-phenotype combination with different frecuency, and
                # only the higher one will be taken.
                # in order to avoid duplicate primary key of the tables and choose the higher frecuency:
                # it keeps all the pk in a list.
                # Other list keeps tuples with the data which will be inserted in the tables.
                # If the pk is already in the pk list, it takes the index where the pk is in the list.
                # If the new frecuency is higher, the row in the index will be delete from the list with the tuple with all the information
                # and the list with the pk.
                # This method is faster than handle the exception and insert data one per one. 

                # UPDATE: primary key that is in the previous version of the table and in the new one
                # If all the data is the same it is repeat data
                # If the data is different is an update
              
                        
                if not drug_se_pk in drug_pe_pk_table:
                    if not drug_se_pk in drug_se_list:
                        drug_se_list.append(drug_se_pk)
                        new_drug_se.append(drug_se)
                        
                    if drug_se_pk in drug_se_list:
                        index=drug_se_list.index(drug_se_pk)
                        if new_drug_se[index][3] < frec_se:
                            new_drug_se.pop(index)
                            drug_se_list.pop(index)  
                            drug_se=(se_id,drug_id,source_id,frec_se,phenotype_type)
                            new_drug_se.append(drug_se)
                            drug_se_list.append(drug_se_pk)
                    
                else:
                    intersection_drug_se.append(drug_se_pk) # Add the pk that is in the previous and the actual version
                    for row in drug_pe_table:
                        PV_drug_se_phenotypeid = row[0]
                        PV_drug_se_drug_id = row[1]
                        PV_frec = row[3]
                        PV_phenotype_type = row[4]
                        if se_id == PV_drug_se_phenotypeid and drug_id == PV_drug_se_drug_id and phenotype_type == PV_phenotype_type:
                            if frec_se > PV_frec:
                                dse_update_values = (frec_se,se_id ,drug_id,phenotype_type)
                                cursor.execute("UPDATE drug_phenotype_effect SET score = '%s' where (phenotype_id = '%s') and (drug_id = '%s') and (phenotype_type = '%s')" % dse_update_values)
# INSERT the data to the database
if new_se:
    cursor.executemany("insert into phenotype_effect values(%s,%s,%s)",new_se)
if new_drug_se:
    cursor.executemany("insert into drug_phenotype_effect values(%s,%s,%s,%s,%s)",new_drug_se)


# DELETE
# primary key that is in the previous version of the table and not in the new one -intersection list-
for row in pe_table:
    PV_se_pk = (row[0],row[1])
    if not PV_se_pk in intersection_se:
        cursor.execute("DELETE FROM phenotype_effect WHERE phenotype_id = '%s' and source_id =  '%s' " % PV_se_pk)

for row in drug_pe_table: 
    PV_dse_pk = (row[0], row[1], row[4])
    if not PV_dse_pk in intersection_drug_se:
        cursor.execute("DELETE FROM drug_phenotype_effect WHERE phenotype_id = '%s' and drug_id =  '%s' and phenotype_type = '%s'" % PV_dse_pk)
