#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul  8 22:15:25 2018

@author: linsam
"""

import sys
import pymysql
import math
sys.path.append('/home/linsam/job')
import Key

def execute_sql2(database,sql_text):
    
    conn = ( pymysql.connect(host = Key.host,# SQL IP
                     port = 3306,
                     user = Key.user,# 帳號
                     password = Key.password,# 密碼
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
#------------------------------------------------------------
'''
self = Crawler2SQL(ACSP.data_name,'StockPrice')
'''        
class Crawler2SQL:   
    
    def __init__(self,dataset_name,database):
        self.host = Key.host
        self.user = Key.user
        self.password = Key.password
        self.dataset_name = dataset_name
        self.database = database

    def creat_sql_file(self,sql_string,database):
        
        conn = ( pymysql.connect(host = self.host,# SQL IP
                                 port = 3306,
                                 user = self.user,# 帳號
                                 password = self.password,# 密碼
                                 database = self.database,  # 資料庫名稱
                                 charset="utf8") )   #  編碼           
        c=conn.cursor()
        c.execute( sql_string )
        c.close() 
        conn.close()
        
    def create_table(self,
                     colname,
                     date_col = ['date','Date','datetime'],
                     text_col = [''],
                     BIGINT_col = [''],
                     dt_col = [''],
                     INT_col = [''],
                     PRIMARYKEY = '',
                     FOREIGNKEY = '',
                     FOREIGNKEY_table = ''):
        # colname = self.index_data.columns
        sql_string = 'create table ' + self.dataset_name + '('
        
        for col in colname:

            if col in date_col:
                sql_string = sql_string + col + ' Date, '               
            elif col in BIGINT_col:
                sql_string = sql_string + col + ' BIGINT(64), '
            elif col in INT_col:
                sql_string = sql_string + col + ' INT(10), '                
            elif col in dt_col:
                sql_string = sql_string + col + ' DATETIME, '             
            elif col == PRIMARYKEY or col == FOREIGNKEY:
                sql_string = sql_string + col + ' VARCHAR(50), ' 
            elif col in text_col:
                sql_string = sql_string + col + ' TEXT(100), '
            else:
                sql_string = sql_string + col + ' FLOAT(16), '                 
        # VARCHAR(11) NOT NULL;
        if PRIMARYKEY != '':
            sql_string = sql_string +' PRIMARY KEY (' + PRIMARYKEY + '), '
        sql_string = sql_string +' FOREIGN KEY (' + FOREIGNKEY + ') '                
        sql_string = sql_string +' REFERENCES ' + FOREIGNKEY_table + '(' + FOREIGNKEY + '), ' 
        
        sql_string = sql_string[:len(sql_string)-2] + ')'
        
        execute_sql2(self.database,"SET GLOBAL FOREIGN_KEY_CHECKS=0;")
        
        self.creat_sql_file(sql_string,self.database)  
        if PRIMARYKEY == '':
            sql_text = 'ALTER TABLE `'+self.dataset_name+'` ADD id BIGINT(64) NOT NULL AUTO_INCREMENT PRIMARY KEY;'
            execute_sql2(self.database,sql_text)
    
    def upload2sql(self,data,no_float_col = ['date','Date','datetime'],INT_col = [''] ):

        def create_upload_value(data,dataset_name,i):
            
            colname = data.columns
            upload_string = ('insert into ' + dataset_name + '(')
            upload_string2 = ''
            value = []
            for col in colname:
                tem = data[col][i] 
                if tem in ['NaT','']:
                    123    
                elif col in INT_col:
                    value.append( int( tem ) )
                    upload_string = upload_string + col+','
                    upload_string2 = upload_string2 + '%s,'                    
                elif col in no_float_col:
                    value.append( tem )            
                    upload_string = upload_string + col+','
                    upload_string2 = upload_string2 + '%s,'
                elif math.isnan(tem):
                    123                     
                else:
                    value.append( float( tem ) )
                    upload_string = upload_string+col+','
                    upload_string2 = upload_string2+'%s,'
                    
            upload_string = upload_string[:len(upload_string)-1] +') values('
            upload_string = upload_string + upload_string2
            upload_string = upload_string[:len(upload_string)-1] + ')'  

            return value, upload_string

        # database = 'Financial_DataSet'
        conn = ( pymysql.connect(host = self.host,# SQL IP
                         port = 3306,
                         user = self.user,
                         password = self.password,
                         database = self.database,  
                         charset="utf8") )             
        data.index = range(len(data))
        for i in range(len(data)):
            if i%500 == 0:
                print(str(i)+'/'+str(len(data)))
            #i = 0
            #upload_string = create_upload_string(data,self.dataset_name,i)
            value, upload_string =  create_upload_value(data,self.dataset_name,i)
            
            ( conn.cursor().execute( upload_string,tuple(value) ) )
             
        conn.commit()
        conn.close()     

    
