import os
import mysql.connector
from multiprocessing.dummy import Pool
from logging import log
mydb = mysql.connector.connect(host='localhost',user='root',password='shr')

def query(sql):
    global mydb
    try:
            cursor = mydb.cursor()
            cursor.execute(sql)
    except Exception:
            mydb = mysql.connector.connect(host='localhost',user='root',password='shr')

            cursor = mydb.cursor()
            cursor.execute(sql)
    res=cursor.fetchall()
    cursor.close()
    mydb.commit()
    return res

schema = {"columns":["Stud_id","Stud_name","Stud_marks"],\

"dtypes":["Number","String","String"]}

shards = ["sh3","sh4"]
def config():
       
    try:
        columns = schema['columns']
        dtypes = schema['dtypes']

        dmap={'Number':'INT','String':'VARCHAR(512)'}
        col_config=''
        for c,d in zip(columns,dtypes):
            col_config+=f", {c} {dmap[d]}"
        for shard in shards:
            # Test this line
            query(f"CREATE DATABASE {shard}")
            query(f"USE {shard}")
            query(f"CREATE TABLE StudT (id INT AUTO_INCREMENT PRIMARY KEY{col_config})")
    except Exception as e:
        log.error(e)

def copy():
    global shards, schema
    columns_list = ",".join(schema['columns'])
    columns = schema['columns']
    response_message = {}
    for shard in shards:
        response = []
        query(f"USE {shard}")
        result = query(f"SELECT {columns_list} FROM SutdT")
        for row in result:
            res = {}
            for i, column in enumerate(columns):
                res[column] = row[i]
            response.append(res)
        print(response)
if __name__ == "__main__":
    #config()
    copy()
    print("Configured")