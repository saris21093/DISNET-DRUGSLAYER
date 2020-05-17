"""This module is for getting the UMLS code and ORPHAN code for the diseases.
It uses the modules get_orhpan_code and get_umls"""

import get_orhpan_code
import get_umls
import conection_DISNET_drugslayer
from get_list import get_list
cursor = conection_DISNET_drugslayer.cursor

# Initialize auxiliaries
count_umls=0
count_orphanet=0

complete_list_code_umls=[]
code_umls_list=[]
has_code_umls_list=[]

complete_list_code_orphan=[]
code_orphan_list=[]
has_code_orphan_list=[]

# Get the list of the diseases
disease_list=get_list("select resource_id, disease_id, disease_name from disease")
orphan_dic=get_orhpan_code.orphan_codes(orphan_dic={})

# Get the list of the ids which have already UMLS id
has_code_table=get_list(" select id from has_code where resource_id = 121")
has_code_table=list(*zip(*has_code_table))

code_table=get_list(" select code from code where resource_id = 121")
code_table=list(*zip(*code_table))

code_table_orphan=get_list(" select code from code where resource_id = 99")
code_table_orphan=list(*zip(*code_table_orphan))


# Resource id
UMLS_RESOURCE_ID = 121
ORPHAN_RESOURCE_ID = 99

# Entity id
DISEASE_ENTITY_ID = 1


for i in disease_list:
    disease_id = i[1]
    disease_name = i[2]

    # There are two different codes in the disease table, OMIM and MeSH
    if i[0] == 72:
        source_cr= 'OMIM'
    if i[0] == 75:
        source_cr = 'MSH'

    # in order to avoid duplicate primary key of the tables, it keeps all the keys in a list,
    # Other list keeps tuples with the data which will be inserted in the tables, 
    # They will be inserted each 500 rows.
    # This method is faster than handle the exception and insert data one per one

    if not disease_id in has_code_table:
        umls_id=get_umls.get_umls(disease_id,disease_name,source_cr)
        if umls_id != None:
            if not umls_id in code_table:
                if not umls_id in complete_list_code_umls:
                    complete_list_code_umls.append(umls_id)
                    code_umls=(umls_id,UMLS_RESOURCE_ID,DISEASE_ENTITY_ID)
                    has_code_umls=(disease_id,umls_id,UMLS_RESOURCE_ID,DISEASE_ENTITY_ID)
                    code_umls_list.append(code_umls)
                    has_code_umls_list.append(has_code_umls)
                    count_umls+=1
        
    if disease_id in orphan_dic:
        orphan_id=orphan_dic[disease_id]
        if orphan_id != None:
            if not orphan_id in code_table_orphan:
                if not orphan_id in complete_list_code_orphan:
                    complete_list_code_orphan.append(orphan_id)
                    code_orphan=(orphan_id,ORPHAN_RESOURCE_ID,DISEASE_ENTITY_ID)
                    has_code_orphan=(disease_id,orphan_id,ORPHAN_RESOURCE_ID,DISEASE_ENTITY_ID)
                    code_orphan_list.append(code_orphan)
                    has_code_orphan_list.append(has_code_orphan)
                    count_orphanet+=1
        
        if count_umls==10:
            cursor.executemany("insert into code values(%s,%s,%s)",code_umls_list)
            cursor.executemany("insert into has_code values(%s,%s,%s,%s)",has_code_umls_list)
            code_umls_list=[]
            has_code_umls_list=[]
            count_umls=0

        if count_orphanet==500:
            cursor.executemany("insert into code values(%s,%s,%s)",code_orphan_list)
            cursor.executemany("insert into has_code values(%s,%s,%s,%s)",has_code_orphan_list)
            code_orphan_list=[]
            has_code_orphan_list=[]
            count_orphanet=0


cursor.executemany("insert into code values(%s,%s,%s)",code_umls_list)
cursor.executemany("insert into has_code values(%s,%s,%s,%s)",has_code_umls_list)
cursor.executemany("insert into code values(%s,%s,%s)",code_orphan_list)
cursor.executemany("insert into has_code values(%s,%s,%s,%s)",has_code_orphan_list)



