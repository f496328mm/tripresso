

import os
path = os.listdir('/home')[0]
import sys
sys.path.append('/home/'+ path +'/job')
from tripresso import BasedClass

'''
self = CrawlerGloriatour()
self.main()
'''

class CrawlerOrangetour(BasedClass.Crawler):
        
    def __init__(self):
        super(CrawlerOrangetour, self).__init__(
        encoding = 'utf-8',
        maxPageALL_url = 'http://www.orangetour.com.tw/EW/GO/GroupList.asp',
        index_data_url = 'http://www.orangetour.com.tw/EW/Services/SearchListData.asp',
        detail_data_url = 'http://www.orangetour.com.tw/EW/GO/GroupDetail.asp?prodCd=' ,
        )
        
def crawler_history():
    
    self = CrawlerOrangetour()
    print('get maxPageALL')
    self.get_maxPageALL()
    print('crawler')
    self.crawler()
    print('finish')
    #-----------------------------------------------------------------------
    print('create table')
    C2S = BasedClass.Crawler2SQL('OrangetourData','tripresso')
    try:
        C2S.create_table(self.index_data.columns,
                         date_col = ['LeavDt'],
                         text_col = ['GrupSnm','GrupCd'],
                         INT_col = ['EstmTotqt','SaleYqt','SaleAm','GrupLn'],
                         PRIMARYKEY = 'GrupCd',
                         FOREIGNKEY = 'GrupCd',
                         FOREIGNKEY_table = 'OrangetourFlightData')
    except:
        123
    #------------------------------------------------------------------    
    print('upload to sql')
    INT_col = ['EstmTotqt','SaleYqt','SaleAm','GrupLn']
    no_float_col = list( self.index_data.columns )
    [ no_float_col.remove(var) for var in INT_col ]
    C2S.upload2sql(self.index_data,
                   INT_col = INT_col,
                   no_float_col = no_float_col
                   )
    #==================================================================
    C2S = BasedClass.Crawler2SQL('OrangetourFlightData','tripresso')
    try:
        C2S.create_table(self.flight_data.columns,
                         dt_col = ['DepartureTime','ArrivalTime'],
                         text_col = ['FlightCompany','Flight','Departure','Destination','GrupCd'],
                         INT_col = ['FlightDay'],
                         FOREIGNKEY = 'GrupCd',
                         FOREIGNKEY_table = 'OrangetourData')
    except:
        123
        
    INT_col = ['FlightDay']
    no_float_col = list( self.flight_data.columns )
    [ no_float_col.remove(var) for var in INT_col ]        
    C2S.upload2sql( self.flight_data,
                   INT_col = INT_col,
                   no_float_col = no_float_col )
'''         
class AutoCrawlerGloriatour(CrawlerOrangetour):
    def __init__(self):
        super(AutoCrawlerGloriatour, self).__init__()    
        self.database = 'tripresso'
        
def auto_crawler_new():
    date_name = 'CrudeOilPrices'
    self = AutoCrawlerGloriatour()
    self.main()

    C2S = BasedClass.Crawler2SQL(date_name,'Financial_DataSet')
    C2S.upload2sql(self.data)
'''

def main(x):
    if x == 'history':
        crawler_history()
    #elif x == 'new':
        # python3 /home/.../CrawlerFinancialStatements.py new
    #    auto_crawler_new()
    
if __name__ == '__main__':
    x = sys.argv[1]# cmd : input new or history
    main(x)



