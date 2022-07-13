import scrapy
from scrapy_splash import SplashRequest
import pandas as pd

URL = 'https://vnexpress.net/covid-19/covid-19-viet-nam'
LUA_PATH = \
    '/home/luan/projects/covid_dashboard/pipelines/vn_etl/1_extract/covid_crawler/covid_crawler/spiders/lua/total.lua'

class TotalSpider(scrapy.Spider):

    name = "total"
    lua_script = ""
    with open(LUA_PATH, 'r') as file:
        lua_script = file.read()    

    def start_requests(self):
        yield SplashRequest(
            url=URL, 
            callback=self.parse, 
            endpoint="execute", 
            args={
                'lua_source': self.lua_script
            }
        )

    def parse(self, response):
        list_of_records = []
        main_xpath = '/html/body/section[5]/div/div[1]/div[1]/div[1]/ul[2]/li'
        for li in response.xpath(main_xpath):
            record_list = []
            for i, div in enumerate(li.xpath('./div')):
                # skip no-use value
                if i == 2 or i == 4:
                    continue
                record_list.append(div.xpath('./text()').get())
            list_of_records.append(tuple(record_list))
        # print(list_of_records)
        columns = ['province', 'total_cases', 'total_deaths']
        
        df = pd.DataFrame.from_records(data=list_of_records, columns=columns)
        print(df)
        df.to_csv(
            path_or_buf=
                '/home/luan/projects/covid_dashboard/pipelines/vn_etl/1_extract/covid_crawler/output/total_by_provinces.csv',
            index=False
        )