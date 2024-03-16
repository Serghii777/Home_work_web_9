import json

import scrapy
from itemadapter import ItemAdapter
from scrapy.crawler import CrawlerProcess
from scrapy.item import Item, Field

import certifi
from mongoengine import connect
from models import Authors, Quotes

class QuoteItem(Item):
    quote = Field()
    author = Field()
    tags = Field()

class AuthorItem(Item):
    fullname = Field()
    born_date = Field()
    born_location = Field()
    description = Field()

class DataPipeline:
    quotes = []
    authors = []

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        if "fullname" in adapter.keys():
            self.authors.append(dict(adapter))
        if "quote" in adapter.keys():
            self.quotes.append(dict(adapter))

    def close_spider(self, spider):
        with open('quotes.json', "w", encoding='utf-8') as fd:
            json.dump(self.quotes, fd, ensure_ascii=False, indent=2)
        with open('authors.json', "w", encoding='utf-8') as fd:
            json.dump(self.authors, fd, ensure_ascii=False, indent=2)

class QuotesSpider(scrapy.Spider):
    name = "get_quotes"
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["https://quotes.toscrape.com/"]
    custom_settings = {"ITEM_PIPELINES": {DataPipeline: 300}}

    def parse(self, response, **kwargs):
        for q in response.xpath("/html//div[@class='quote']"):
            quote = q.xpath("span[@class='text']/text()").get().strip()
            author = q.xpath("span/small[@class='author']/text()").get().strip()
            tags = q.xpath("div[@class='tags']/a/text()").extract()
            yield QuoteItem(quote=quote, author=author, tags=tags)
            yield response.follow(url=self.start_urls[0] + q.xpath("span/a/@href").get(), callback=self.parse_author)

        next_link = response.xpath("/html//li[@class='next']/a/@href").get()
        if next_link:
            yield scrapy.Request(url=self.start_urls[0] + next_link)

    def parse_author(self, response, **kwargs):
        content = response.xpath("/html//div[@class='author-details']")
        fullname = content.xpath("h3[@class='author-title']/text()").get().strip()
        born_date = content.xpath("p/span[@class='author-born-date']/text()").get().strip()
        born_location = content.xpath("p/span[@class='author-born-location']/text()").get().strip()
        description = content.xpath("div[@class='author-description']/text()").get().strip()
        yield AuthorItem(fullname=fullname, born_date=born_date, born_location=born_location, description=description)



# З'єднання з базою даних MongoDB Atlas
connect("work9", host="mongodb+srv://user19:456123@clusterdbgoit.xlgrzju.mongodb.net/", ssl=True, tlsCAFile=certifi.where())

def load_authors_from_json(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        authors_data = json.load(file)
        for author_data in authors_data:
            Authors(**author_data).save()

def load_quotes_from_json(filename):
    with open(filename, 'r', encoding='utf-8') as file:
        quotes_data = json.load(file)
        for quote_data in quotes_data:
            author = Authors.objects(fullname=quote_data['author']).first()
            if author:
                quote_data['author'] = author
                Quotes(**quote_data).save()


if __name__ == '__main__':
    process = CrawlerProcess()
    process.crawl(QuotesSpider)
    process.start()
    load_authors_from_json('authors.json')
    load_quotes_from_json('quotes.json')