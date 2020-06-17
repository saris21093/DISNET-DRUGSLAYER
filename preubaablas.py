import mysql.connector
from mysql.connector import errorcode
DB_NAME = 'drugs'

cnx = mysql.connector.connect(host='localhost',
                            user='root',
                            password='Barcelona21093',
                            database = DB_NAME)
cursor = cnx.cursor()
cnx.autocommit = True
#cursor.execute("insert into code_reference values(72,'OMIM')")
cursor.execute("ALTER TABLE disease ADD resource_id int FIRST")
cursor.execute("UPDATE disease SET resource_id = 75 WHERE resource_id is Null  ")
cursor.execute("select resource_id from disease")
tabla=cursor.fetchall()
print((tabla))