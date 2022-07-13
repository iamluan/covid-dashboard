# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import pymongo


class MongoPipeline:

    collection_name = 'scraped_covid'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
        )

    def open_spider(self, spider):
        if spider.name == 'total':
            return
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.db[spider.collection_name].drop()

    def close_spider(self, spider):
        if spider.name == 'total':
            return
        self.client.close()

    def process_item(self, item, spider):
        if spider.name == 'total':
            return
        self.db[spider.collection_name].insert_one(ItemAdapter(item).asdict())
        return item