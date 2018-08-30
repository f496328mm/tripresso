# -*- coding: utf-8 -*-
"""
Created on Thu Aug 30 19:59:08 2018

@author: sam
"""

import os
path = os.listdir('/home')[0]
import sys
import pymysql
sys.path.append('/home/'+ path +'/job')
import Key

password = '*********' # mysql_password

def CreateDataBase(dbname):# dbname = 'test'

    conn = ( pymysql.connect(host = Key.host,# SQL IP
                     port = 3306,
                     user = 'root',# 帳號
                     password = password,# 密碼
                     charset="utf8") )   #  編碼
                             
    cursor = conn.cursor()    
    sql_text = "CREATE DATABASE " + dbname + ";"
    cursor.execute(sql_text)
    #--------------------------------------------------------------------------------------------
    sql_text = "ALTER DATABASE  `" + dbname + "` DEFAULT CHARACTER SET='utf8';"
    cursor.execute(sql_text)
    sql_text = "ALTER DATABASE `" + dbname + "` DEFAULT Collate='utf8_unicode_ci';"
    cursor.execute(sql_text)
    conn.close()
#-----------------------------------------------------------------------------
CreateDataBase('tripresso')

