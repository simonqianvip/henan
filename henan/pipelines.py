# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging

import MySQLdb
import MySQLdb.cursors
from twisted.enterprise import adbapi

logger = logging.getLogger(__name__)
class HenanPipeline(object):
    def process_item(self, item, spider):
        return item



class MySQLStoreHenanPipeline(object):
    """
    数据存储到mysql
    """
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        '''
        从settings文件加载属性
        :param settings:
        :return:
        '''
        dbargs = dict(
                host=settings['MYSQL_HOST'],
                db=settings['MYSQL_DBNAME'],
                user=settings['MYSQL_USER'],
                passwd=settings['MYSQL_PASSWD'],
                charset='utf8',
                cursorclass=MySQLdb.cursors.DictCursor,
                use_unicode=True,
        )
        dbpool = adbapi.ConnectionPool('MySQLdb', **dbargs)
        return cls(dbpool)

    # pipeline默认调用
    def process_item(self, item, spider):
        deferred = self.dbpool.runInteraction(self._do_insert, item, spider)
        deferred.addErrback(self._handle_error)
        # d.addBoth(lambda _: item)
        return deferred

    # 将每行更新或写入数据库中
    def _do_insert(self, conn, item, spider):
        """
        CREATE TABLE `henan_info` (
          `id` int NOT NULL AUTO_INCREMENT COMMENT '主键',
          `title` varchar(100) COMMENT "名称",
          `link` varchar(200) COMMENT '链接',
          `pdate` VARCHAR(50)  COMMENT '日期',
          PRIMARY KEY (`id`)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8;
        """
        conn.execute("""
                insert into henan_info(title, link, pdate,titleId)
                values(%s, %s, %s,%s)
                """, (item['title'],item['link'] ,item['date'],item['titleId']))
        logger.info('insert into success')

    def _handle_error(self, failue):
        logger.error(failue)