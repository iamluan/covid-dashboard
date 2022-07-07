import scrapy
from scrapy_splash import SplashRequest

URL = 'https://covid19.gov.vn/big-story/cap-nhat-dien-bien-dich-covid-19-moi-nhat-hom-nay-171210901111435028.htm'
LUA_PATH = \
    '/home/luan/projects/covid_dashboard/pipelines/vn_etl/1_extract/covid_crawler/covid_crawler/spiders/lua/lua_script.lua'

class CovidSpider(scrapy.Spider):

    name = "covid"
    collection_name = 'scrapped'

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
        # print(response.body)
        main_xpath = '//body//div[@id="admwrapper"]//div[@class="main"]//div[@class="list__main"]//div//div//div[@class="layout__main-content"]//div//div//div[@class="detail-main"]//div[@class="bigstories"]//div//div[2]//ul[2]/li[position()>1]'
        # print(response.xpath(main_xpath))
        date_xpath = f'.//div//div/@data-date'
        content_xpath = f'.//div[@class="kbwscwl-content clearfix"]//p[2]/text()'
        for li in response.xpath(main_xpath):
            # print(li)
            yield {
                'date': li.xpath(date_xpath).get(),
                'content': li.xpath(content_xpath).get()
            }