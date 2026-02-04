# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import hashlib
import pymongo
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

# pipeline: connect db -> insert data 
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

    # insert data to db (avoid adding dup entries)
    def process_item(self, item, spider):
        item_id = self.compute_item_id(item)
        item_dict = ItemAdapter(item).asdict()

        # Update if exist, otherwise create
        self.db[self.COLLECTION_NAME].update_one(
            filter={"_id": item_id},
            update={"$set": item_dict},
            upsert=True # create if item doesnt exist
        )

        return item
    
        # Skip if exist, otherwise create
        # if self.db[self.COLLECTION_NAME].find_one({"_id": item_id}):
        #     raise DropItem(f"Duplicate item found: {item}") # tells the framework to discard this item and not to process it further
        # else:
        #     item["_id"] = item_id
        #     self.db[self.COLLECTION_NAME].insert_one(ItemAdapter(item).asdict())
        #     return item

    
    def compute_item_id(self, item):
        url = str(item["url"])
        return hashlib.sha256(url.encode("utf-8")).hexdigest()
