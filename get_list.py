"""Get list from the database's tables.
"""

import conection_DISNET_drugslayer
def get_list(sql_statement):
    cursor = conection_DISNET_drugslayer.cursor
    """ This function is for get list from MySQL tables, 
    the input parameters are the SELECT sql statement"""
    cursor.execute(sql_statement)
    table_list=cursor.fetchall()
    return table_list

