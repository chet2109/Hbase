
# coding: utf-8

# In[ ]:

import happybase
import pandas as pd
import numpy
import json
from io import StringIO


# In[ ]:

class Hbase:
    def __init__(self,host,table_name):
        """ As a part object creation connection will be established with desired host and table name"""
        self.conn = happybase.Connection(host = host,table_prefix = "default",table_prefix_separator = ":",autoconnect=True)
        self.table = self.conn.table(table_name)
        
    
    def insert_row(self, row):
        """ Insert a row into HBase. rows will besent to the database individually.Rows have the following schema:
        [ id	typ	time	intent	entity	city	state	country	restname	paytype	year	month	date]
        """
        table.put(row[0], { "data:typ": row[1], "data:time": row[2], "data:intent": row[3],
        "data:entity": row[4], "data:city": row[5], "data:state": row[6],"data:country": row[7],"data:restname": row[8], 
                  "data:paytype": row[9],"data:year": row[10],"data:month": row[11],"data:date": row[12]})
    
    def default(self,o):
        """ Memeber method to convert numpy int to standard int as jsonify fails for numpy int"""
        if isinstance(o, numpy.int64): 
            return int(o)  
        raise TypeError
    
    def process_data(self,rows):
        """ Preprocessing required for converting to nested json"""
        df = pd.DataFrame()
        for row in rows:
            df_row = {key.decode('utf8').split(':')[1]: value.decode('utf8') for key, value in row[1].items()}
            df = df.append(df_row, ignore_index=True)
        df1 = df.groupby(['typ','intent','month']).size()
        df3 = df1.to_csv()
        TESTDATA = StringIO(df3)
        df4 = pd.read_csv(TESTDATA, sep=",",header=None)
        df4.rename(columns={0: 'typ', 1: 'intent',2: 'month',3: 'count'},inplace=True)
        return(df4)
        
    
    def fetch_row(self, coloumn_family):
        """Fetch the entire table into and then will be processed in process_data method as data frame"""
        rows = self.table.scan(columns=[coloumn_family])
        return(rows)
    
    def retro_dictify(self,frame):
        """Converts a data frame to nested dictionary in the order of data frame columns"""
        d = {}
        for row in frame.values:
            here = d
            for elem in row[:-2]:
                if elem not in here:
                    here[elem] = {}
                here = here[elem]
            here[row[-2]] = row[-1]
        return d


# In[ ]:

""" This is an example of how to generate an nested json object from the above class object
   hbase = Hbase("0.0.0.0","REST")
   d7 = hbase.fetch_row(b'data')
   d11 = hbase.process_data(d7)
   j1= hbase.retro_dictify(d11)
   x6=json.dumps(j1, default= hbase.default)
"""





