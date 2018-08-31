

import pandas as pd
import numpy as np
import pymysql
host = '114.32.60.100'
#host = 'localhost'
user = 'guest'
password = '123'

#---------------------------------------------------------

def execute_sql2(database,sql_text):
    
    conn = ( pymysql.connect(host = host,# SQL IP
                     port = 3306,
                     user = user,# 帳號
                     password = password,# 密碼
                     database = database,  # 資料庫名稱
                     charset="utf8") )   #  編碼
                             
    cursor = conn.cursor()    
    # sql_text = "SELECT * FROM `_0050_TW` ORDER BY `Date` DESC LIMIT 1"
    try:   
        cursor.execute(sql_text)
        data = cursor.fetchall()
        conn.close()
        return data
    except:
        conn.close()
        return ''    
#---------------------------------------------------------
# based class 
'''
self = LoadData()
'''
class LoadData:
    def __init__(self,database = '',data_name = ''):
        self.data_name = data_name        
        self.database = database     
        self.list_col_name = ''
        self.list_data_name = ''
        self.drop_col = []
        
    def get_col_name(self):
       
        tem_col_name = execute_sql2(
                database = self.database,
                sql_text = 'SHOW columns FROM '+ self.data_name )
    
        col_name = []
        for i in range(len(tem_col_name)):
            col_name.append( tem_col_name[i][0] )
        
        for drop in self.drop_col:
            col_name.remove(drop)
        
        self.col_name = col_name
    
    def execute2sql(self,text,col=''):
        tem = execute_sql2(
            database = self.database,
            sql_text = text)
        
        if col=='Date':
            tem = [np.datetime64(x[0]) for x in tem]
            tem = pd.DataFrame(tem)
            self.data[col] = tem.loc[:,0]
        else:
            tem = np.concatenate(tem, axis=0)
            tem = pd.DataFrame(tem)
            self.data[col] = tem.T.iloc[0]
    #-----------------------------------------------------
    def get_data(self,all_data = ''):
        
        self.data = pd.DataFrame()
        for j in range(len(self.col_name)):
            #print(j)
            col = self.col_name[j]
            text = 'select ' + col + ' from ' + self.data_name
            
            if all_data == 'T': 
                123

            self.execute2sql(text,col)

        return self.data

    def datalist(self):
        
        tem = execute_sql2(
                database = self.database,
                sql_text = 'SELECT distinct `' + self.list_col_name + '` FROM `' + self.data_name + '`')
        value = [ te[0] for te in tem ]
        return value
        
    def load_all(self):
        
        self.get_col_name()
        self.data = self.get_data(all_data='T')
        
#---------------------------------------------------------------
'''
class ClassOrangetourData(LoadData):
    def __init__(self):
        super(ClassOrangetourData, self).__init__(
                database = 'tripresso',
                data_name = 'OrangetourData')

    def load_all(self):
        
        self.get_col_name()
        self.data = self.get_data(all_data='T')

        return self.data
'''  
def OrangetourData():
    
    self = LoadData(database = 'tripresso',data_name = 'OrangetourData')  
    self.load_all()
    
    return self.data

#---------------------------------------------------------------

# GloriatourData
# GloriatourFlightData
# OrangetourData
# OrangetourFlightData


def Load(data_name = ''):
    database = 'tripresso'
    
    tem = execute_sql2(database,'SHOW TABLES')# 
    sql_data_name = [ te[0] for te in tem ]
    
    if  data_name in sql_data_name:
        
        self = LoadData(database = database,data_name = data_name)  
        self.load_all()
    else:
        print( 'data_name ' + data_name + " doesn't exist" )
        raise(AttributeError, "Hidden attribute")  

    return self.data
    #123
#---------------------------------------------------------
    
'''

import sys
sys.path.append('/home/sam/job')
from tripresso.OpenData.Load import Load

# 'GloriatourData',
# 'GloriatourFlightData',
# 'OrangetourData',
# 'OrangetourFlightData'

GloriatourData = Load(data_name = 'GloriatourData')
GloriatourFlightData = Load(data_name = 'GloriatourFlightData')
OrangetourData = Load(data_name = 'OrangetourData')
OrangetourFlightData = Load(data_name = 'OrangetourFlightData')



'''





    

