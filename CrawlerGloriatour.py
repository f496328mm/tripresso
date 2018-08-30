

import os
path = os.listdir('/home')[0]
import requests
import sys
from bs4 import BeautifulSoup
import pandas as pd
sys.path.append('/home/'+ path +'/job')
from tripresso import BasedClass

'''

self = CrawlerGloriatour()
self.crawler()

'''

class CrawlerGloriatour:
    
    def get_maxPageALL(self):
        index_url = 'https://www.gloriatour.com.tw/EW/GO/GroupList.asp'
        res = requests.get(index_url,verify = True)  
        res.encoding = 'big5' 
        soup = BeautifulSoup(res.text, 'lxml')

        tem = soup.find('input',{'name':'maxPageALL'})
        self.maxPageALL = tem['value']
        
    
    def get_value(self,i):# i = 2
        def get_index_value(i):
            
            from_data = {'pageALL': str(i)}
            
            index_url = 'https://www.gloriatour.com.tw/EW/Services/SearchListData.asp'
            res = requests.post(index_url,verify = True,data = from_data)  
            res.encoding = 'big5'       
            
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
            tem = 'https://www.gloriatour.com.tw/EW/GO/GroupDetail.asp?prodCd=' 
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
                res.encoding = 'big5'       
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
        #for i in range(1,int(self.maxPageALL)+1):# i = 1
        for i in range(1,3):# i = 2
            print( str(i) + '/' + self.maxPageALL )
            v1, v2 = self.get_value(i)
            self.index_data = self.index_data.append(v1)
            self.flight_data = self.flight_data.append(v2)
            
    def main(self):
        print('get max Page ALL')
        self.get_maxPageALL()
        print('crawler')
        self.crawler()
        self.index_data.index = range(len(self.index_data))
        self.flight_data.index = range(len(self.flight_data))
        print('finish')

class AutoCrawlerGloriatour(CrawlerGloriatour):
    def __init__(self):
        super(AutoCrawlerGloriatour, self).__init__()    
        self.database = 'tripresso'
        
def crawler_history():
    
    self = CrawlerGloriatour()
    self.main()
    #-----------------------------------------------------------------------
    C2S = BasedClass.Crawler2SQL('GloriatourData','tripresso')
    try:
        C2S.create_table(self.index_data.columns,
                         date_col = ['LeavDt'],
                         text_col = ['GrupSnm','GrupCd'],
                         INT_col = ['EstmTotqt','SaleYqt','SaleAm','GrupLn'],
                         PRIMARYKEY = 'GrupCd',
                         FOREIGNKEY = 'GrupCd',
                         FOREIGNKEY_table = 'GloriatourFlightData')
    except:
        123
    #------------------------------------------------------------------    
    INT_col = ['EstmTotqt','SaleYqt','SaleAm','GrupLn']
    no_float_col = list( self.index_data.columns )
    [ no_float_col.remove(var) for var in INT_col ]
    C2S.upload2sql(self.index_data,
                   INT_col = INT_col,
                   no_float_col = no_float_col
                   )
    #==================================================================
    C2S = BasedClass.Crawler2SQL('GloriatourFlightData','tripresso')
    try:
        C2S.create_table(self.flight_data.columns,
                         dt_col = ['DepartureTime','ArrivalTime'],
                         text_col = ['FlightCompany','Flight','Departure','Destination','GrupCd'],
                         INT_col = ['FlightDay'],
                         FOREIGNKEY = 'GrupCd',
                         FOREIGNKEY_table = 'GloriatourData')
    except:
        123
        
    INT_col = ['FlightDay']
    no_float_col = list( self.flight_data.columns )
    [ no_float_col.remove(var) for var in INT_col ]        
    C2S.upload2sql( self.flight_data,
                   INT_col = INT_col,
                   no_float_col = no_float_col )
    
def auto_crawler_new():
    date_name = 'CrudeOilPrices'
    self = AutoCrawlerGloriatour()
    self.main()

    C2S = BasedClass.Crawler2SQL(date_name,'Financial_DataSet')
    C2S.upload2sql(self.data)
 

def main(x):
    if x == 'history':
        crawler_history()
    elif x == 'new':
        # python3 /home/.../CrawlerFinancialStatements.py new
        auto_crawler_new()
    
if __name__ == '__main__':
    x = sys.argv[1]# cmd : input new or history
    main(x)



