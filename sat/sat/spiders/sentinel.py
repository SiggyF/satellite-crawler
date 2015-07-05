# -*- coding: utf-8 -*-
import logging

from scrapy.spiders import XMLFeedSpider, Spider, CrawlSpider, Rule
from scrapy.spiders import CrawlSpider, Rule
from scrapy.http import Request
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from scrapy.utils.spider import iterate_spider_output
from scrapy.utils.conf import get_config
from scrapy.loader.processors import TakeFirst

from scrapy.loader import ItemLoader
from sat.items import SatItem

domain = 'scihub.esa.int'
polygon = "POLYGON((-4.53 29.85,26.75 29.85,26.75 46.80,-4.53 46.80,-4.53 29.85))"


class SentinelSpider(XMLFeedSpider):
    def __init__(self, settings, polygon=polygon, *args, **kwargs):
        """construct with settings"""
        self.settings = settings
        self.logger.info("polygon %s", polygon)
        self.start_urls = [
            'https://' + domain +
            '/dhus/api/search?q=footprint:' +
            '"Intersects({polygon})"'.format(polygon=polygon)
        ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """expose the settings"""
        settings = crawler.settings
        spider = cls(settings, *args, **kwargs)
        spider._set_crawler(crawler)
        return spider

    name = 'sentinel'
    allowed_domains = [domain]

    rules = [
        # Extract links matching 'category.php' (but not matching 'subsection.php')
        # and follow links from them (since no callback means follow=True by default).
        Rule(
            LxmlLinkExtractor(
                allow="/dhus/api/search",
                tags="{http://www.w3.org/2005/Atom}link",
                attrs=("href", )
            )
        )
    ]

    # the iternode iterator does not work because the xml is using nested xml
    iterator = 'xml'
    itertag = 'atom:entry'
    namespaces = [
        ('atom', 'http://www.w3.org/2005/Atom'),
        ('opensearch', 'http://a9.com/-/spec/opensearch/1.1/')
    ]
    # get passwords from config
    http_user = get_config().get(domain, 'username')
    http_pass = get_config().get(domain, 'password')

    def parse_nodes(self, response, nodes):
        """
        Inherited from XMLFeedSpider
        Extended to also return requests.
        """
        for selector in nodes:
            ret = iterate_spider_output(self.parse_node(response, selector))
            for result_item in self.process_results(response, ret):
                yield result_item
        seen = set()
        for i, rule in enumerate(self.rules):
            links = [
                l
                for l
                in rule.link_extractor.extract_links(response)
                if l not in seen
            ]
            self.logger.info('links %s', links)
            if links and rule.process_links:
                links = rule.process_links(links)
            for link in links:
                seen.add(link)
                r = Request(url=link.url)
                r.meta.update(rule=i, link_text=link.text)
                yield rule.process_request(r)


    def parse_node(self, response, selector):
        self.logger.info("selector %s", selector )
        l = ItemLoader(SatItem(), selector=selector, response=response)
        l.default_output_processor = TakeFirst()
        l.add_xpath("metadata", "atom:link[@rel='alternative']/@href")
        l.add_xpath("download", "atom:link/@href")
        l.add_xpath('footprint', "atom:str[@name='footprint']/text()")
        l.add_xpath('id', 'atom:id/text()')
        l.add_xpath('identifier', "atom:str[@name='identifier']/text()")
        i = l.load_item()
        return i


