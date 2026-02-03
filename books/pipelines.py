# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import pymongo
from itemadapter import ItemAdapter

class MongoPipline:
    COLLECTION_NAME = "books"

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
    
    # create pipeline instance, using db config in settings.py
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URL"),
            mongo_db=crawler.settings.get("MONGO_DATABASE"),
        )
        
    # gets called when the spider first run (connect MongoDB, choose db)
    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    # gets called when the spider closes 
    def close_spider(self, spider):
        self.client.close()

    # insert data to db
    def process_item(self, item, spider):
        self.db[self.COLLECTION_NAME].insert_one(ItemAdapter(item).asdict())
        return item
