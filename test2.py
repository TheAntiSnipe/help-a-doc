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
data2=[]
final_index = []
for x in range(10):
    list1=[]
    indices = []
    text = str(df8.values[x]).lower().split(" ")
    for j in text:
        if j in j_son:
            list1.append(j)
            indices.append(j_son.index(j))
    data1 = list(set(list1))
    symptom_list_index = sorted(list(set(indices)))
    data2.append(data1)
    csv_list = []
    for con in range(0,540):            #Finding whether symptoms are present or not
        if con in symptom_list_index:   #If present append 1
            csv_list.append(1)
        else:                           #If not then append 0
            csv_list.append(0)
    final_index.append(csv_list)        #Final symptom list that has either 0 or 1
    data = {'Symptoms':data2}

df = pd.DataFrame(data)
df
