#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul  8 22:15:25 2018

@author: linsam
"""

import sys
import pymysql
import math
import requests
from bs4 import BeautifulSoup
import pandas as pd
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
class Crawler:
    
    def __init__(self,encoding,maxPageALL_url,index_data_url,detail_data_url):
        #self.encoding = 'utf-8'
        #self.maxPageALL_url = 'http://www.orangetour.com.tw/EW/GO/GroupList.asp'
        #self.index_data_url = 'http://www.orangetour.com.tw/EW/Services/SearchListData.asp'
        #self.detail_data_url = 'http://www.orangetour.com.tw/EW/GO/GroupDetail.asp?prodCd=' 
        self.encoding = encoding
        self.maxPageALL_url = maxPageALL_url
        self.index_data_url = index_data_url
        self.detail_data_url = detail_data_url
        self.databased = 'tripresso'
    
    def get_maxPageALL(self):
        #index_url = 'http://www.orangetour.com.tw/EW/GO/GroupList.asp'
        res = requests.get(self.maxPageALL_url,verify = True)  
        res.encoding = self.encoding
        soup = BeautifulSoup(res.text, 'lxml')

        tem = soup.find('input',{'name':'maxPageALL'})
        self.maxPageALL = tem['value']
        
    
    def get_value(self,i):# i = 2
        def get_index_value(i):
            
            from_data = {'pageALL': str(i)}
            
            #index_url = 'http://www.orangetour.com.tw/EW/Services/SearchListData.asp'
            res = requests.post(self.index_data_url,verify = True,data = from_data)  
            res.encoding = self.encoding  
            
            data = res.json()
            index_data = pd.DataFrame(data['All'])
            select_column = ['GrupCd',# id
                             'GrupSnm',# 行程名稱 (eg. 奢華雅儷北越雙龍灣天堂雅儷號海上Villa五日-五星飯店/不走購物站)
                             'LeavDt',# 出發日期 (eg. 08/24)
                             'EstmTotqt',# 總團位 (eg. 18)
                             'SaleYqt',# 可售位 (eg. 17)
                             'SaleAm',# 價錢 (eg. 33000)
                             'GrupLn',# 旅遊天數 (eg. 5天)
                             #'WeekDay',# 出發星期幾
                             ]
            index_data = index_data[select_column]
        
            return index_data
        #----------------------------------------------------------------
        def get_detail_value(index_data):
            def get_flight_value(text): # text = res.text
                text = text.replace('\r','').replace('\t','').split('\n')
                text2 = ''
                bo = 0
                for tex in text:
                    if bo == 1:
                        text2 = text2 + tex.replace('  ','') + '\n'
                        
                    if '<ul class="flight_content">' in tex:
                        bo = 1
                        text2 = text2 + tex.replace('  ','') + '\n'
                        
                    elif '</ul>' in tex:
                        bo = 0
                #------------------------------------------------
                soup = BeautifulSoup(text2, "lxml")              
                
                flight_data = pd.DataFrame()
                for tem2 in soup.find_all('ul'):
                    #print(tem2)
                    tem2 = tem2.find_all('li')
                    value = pd.DataFrame( [ te.text for te in tem2 ] ).T
                    flight_data = flight_data.append(value)
                    
                return flight_data
            #----------------------------------------------------------------
            # http://www.orangetour.com.tw/EW/GO/GroupDetail.asp?prodCd=ICN180831KEE
            #tem = 'https://www.gloriatour.com.tw/EW/GO/GroupDetail.asp?prodCd=' 
            tem = self.detail_data_url
            url_set = [ tem + i for i in index_data['GrupCd'] ]
            
            flight_data = pd.DataFrame()
            flight_colname = ['FlightDay',# 天數
                              'FlightCompany',# 航空公司
                              'Flight',# 航班
                              'Departure',# 出發地
                              'DepartureTime',# 起飛時間
                              'Destination',# 目的地
                              'ArrivalTime',# 抵達時間
                              'GrupCd',# FOREIGNKEY
                              #'GrupCd_Flight'# PRIMARYKEY
                              ]            
            for url in url_set:# url = url_set[0]
                res = requests.get(url,verify = True)  
                res.encoding = self.encoding
                #soup = BeautifulSoup(res.text, "lxml")
                
                value = get_flight_value(res.text)
                GrupCd = url.replace(tem,'')
                value['GrupCd'] = GrupCd
                #value['GrupCd_Flight'] = [ GrupCd + '-' + v for v in value[2] ]
                flight_data = flight_data.append( value )
                
        #----------------------------------------------------------------
            flight_data.columns = flight_colname
            flight_data.index = range(len(flight_data))

            #---------------------------------------------------------
            #detail_data = flight_data
            return flight_data
            
        #=======================main====================================
        index_data = get_index_value(i)
        flight_data = get_detail_value(index_data)
        # final_data = 
        return index_data,flight_data
    
    def crawler(self):
        
        self.index_data, self.flight_data = [pd.DataFrame() for i in range(2) ]
        for i in range(1,int(self.maxPageALL)+1):# i = 1
        #for i in range(1,3):# i = 2
            print( str(i) + '/' + self.maxPageALL )
            v1, v2 = self.get_value(i)
            self.index_data = self.index_data.append(v1)
            self.flight_data = self.flight_data.append(v2)
            
        self.index_data.index = range(len(self.index_data))
        self.flight_data.index = range(len(self.flight_data))
        
        
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

    
