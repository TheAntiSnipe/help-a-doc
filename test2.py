import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
import json
import os

def execute_query_safely(sql, con):
    cur = con.cursor()
    # try to execute the query
    try:
        cur.execute(sql)
    except:
        # if an exception, rollback, rethrow the exception - finally closes the connection
        cur.execute('rollback;')
        raise
    finally:
        cur.close()
    return

aline_path = 'C:/Users/Pranav/Desktop/Code/projects/help-a-doc/help-a-doc/aline/'
concepts_path = 'C:/Users/Pranav/Desktop/Code/projects/help-a-doc/help-a-doc/concepts/'
con = psycopg2.connect(dbname="mimic", user="postgres", password="123abc456", host="localhost")
query_schema = 'SET SEARCH_PATH TO public,' + 'mimiciii' + ';'
query = query_schema + '''
SELECT text
FROM noteevents
LIMIT 10
'''
df8 = pd.read_sql_query(query, con)

with open('C:/Users/Pranav/Desktop/Code/projects/help-a-doc/help-a-doc/symptoms.json') as f:
    j_son = json.load(f)

with open('symptoms_list.csv','a') as f:
    f.write("symptom1,symptom2,symptom3,symptom4,symptom5,symptom6,symptom7,symptom8,symptom9,symptom10,symptom11,symptom12,symptom13,symptom14,symptom15\n")
    for x in range(10):
        list1=[]
        text = str(df8.values[x]).lower().split(" ")
        for j in text:
            if j in j_son:
                list1.append(j)
        data1 = sorted(list(set(list1)))
        if len(data1) == 0:
            continue
        elif len(data1) < 15:
            for con in range(len(data1),16):
                data1.append('NaN')
        f.write(",".join(data1))
        f.write("\n")
