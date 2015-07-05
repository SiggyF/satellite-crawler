# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.utils.conf import get_config

import pika.credentials
import pika
import json

import logging

class DuplicatePipeline(object):
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if item['id'] in self.ids_seen:
            raise DropItem("Duplicate item found: %s" % item)
        else:
            self.ids_seen.add(item['id'])
            return item


class RabbitMQPipeline(object):
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Constructing rabbitmq logger")
        username = get_config().get('rabbitmq', 'username')
        password = get_config().get('rabbitmq', 'password')
        credentials = pika.credentials.PlainCredentials(
            username=username,
            password=password
        )
        host = get_config().get('rabbitmq', 'host')
        parameters = pika.ConnectionParameters(
            host=host,
            port=5672,
            virtual_host='/',
            credentials=credentials
        )
        connection = pika.BlockingConnection(
            parameters=parameters
        )
        channel = connection.channel()
        channel.queue_declare('crisis_crawl')
        self.channel = channel
    def process_item(self, item, spider):
        self.logger.info('sending message')
        serialized = json.dumps(dict(item))
        self.channel.basic_publish(
            exchange='',
            routing_key='crisis_crawl',
            body='%s' % (serialized,)
        )
        return item
