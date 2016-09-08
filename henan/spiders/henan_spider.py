# -*- coding: utf-8 -*-
import scrapy
from scrapy.selector import Selector
import scrapy.cmdline
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
import time
from scrapy.http import Request, FormRequest
import codecs
import urllib
import logging
from henan.items import HenanItem
import traceback
import MySQLdb
import MySQLdb.cursors
from henan.util.mysql_util import SqlUtil

logger = logging.getLogger(__name__)

localDir = 'd:\\henan\\'

class HenanSpider(scrapy.Spider):
    def __init__(self):
        self.sqlutil = SqlUtil('127.0.0.1', 'root', '123456', 'fm')
        self.sqlutil.connect()
        logger.info(u'初始化数据库连接')

    name = "henan"
    allowed_domains = ["henan.gov.cn"]
    start_urls = [
        'http://www.henan.gov.cn/zwgk/zcjd/'
    ]

    page_index = 1

    def parse(self, response):
        driver = webdriver.PhantomJS()
        driver.get(response.url)
        driver.log_types
        flag = True

        items = []
        while(flag):
            try:
                # TODO 获取主页面的handle
                current_window_handle = driver.current_window_handle
                print 'current_window_handle = %s'%current_window_handle

                tr_tags = driver.find_elements_by_xpath('/html/body/table[9]/tbody/tr/td[2]/table/tbody/tr[3]/td/table/tbody/tr')
                for i in range(1,tr_tags.__len__()+1):

                    # 获取标题和link
                    # a_xpath = '/html/body/table[9]/tbody/tr/td[2]/table/tbody/tr[3]/td/table/tbody/tr[%d]/td/table/tbody/tr/td[2]/a'%i
                    a_xpath = '/html/body/table[9]/tbody/tr/td[2]/table/tbody/tr[3]/td/table/tbody/tr[%d]/td/table/tbody/tr/td[2]'%i
                    a_tag = driver.find_element_by_xpath(a_xpath)
                    title = a_tag.text
                    # print 'title = %s'%title
                    # 根据text定位标签
                    link_text = driver.find_element_by_link_text(title)
                    link = link_text.get_attribute('href')
                    # print 'href = %s'%link
                    # logger.info('href = %s',link)

                    # 获取日期
                    td_xpath = '/html/body/table[9]/tbody/tr/td[2]/table/tbody/tr[3]/td/table/tbody/tr[%d]/td/table/tbody/tr/td[3]'%i
                    td_tag = driver.find_element_by_xpath(td_xpath)
                    date = td_tag.text
                    # print 'text = %s,href = %s,date = %s'%(title,href,date)
                    logger.info('title = %s ,date = %s ,href = %s ',title,date,link)

                    # TODO 连接mysql
                    # 连接mysql数据库，并查询企业名称

                    query_sql = '''select * from henan_info where link = "%s" ''' %link
                    results = self.sqlutil.get_data_from_db(query_sql)

                    if results:
                        logger.info('------database is exist------')
                    else:
                        logger.info('------into second page------')
                        # 如果二级页面是正常页面
                        if 'http://www.henan.gov.cn/zwgk/system' in link:
                            #  TODO 用phantomjs处理二级页面
                            link_text.click()
                            # 获取全部窗口
                            handles = driver.window_handles
                            # print 'all handles = %s'%handles

                            for h in handles:
                                if h != current_window_handle:
                                    driver.switch_to_window(h)

                            tbody = driver.find_element_by_xpath('/html/body/table[10]/tbody/tr[6]')
                            content = tbody.text
                            #   TODO 政策信息写入到txt文件
                            splits = link.split('/')
                            lastStr = splits[splits.__len__()-1]
                            id = lastStr.split('.')
                            titleId = id[0]
                            # 文件内容写到txt文件
                            fileName = localDir + titleId + '.txt'

                            logger.info('fileName = %s',fileName)

                            f = codecs.open(fileName, 'wb', encoding='utf-8')
                            f.write(content)
                            f.close()

                            item = HenanItem()
                            item['title'] = title
                            item['link'] = link
                            item['date'] = date
                            item['titleId'] = titleId
                            items.append(item)
                            # 关闭当前二级页面
                            driver.close()
                            # 切回到主界面
                            driver.switch_to_window(current_window_handle)

                pages = driver.find_elements_by_xpath('/html/body/table[9]/tbody/tr/td[2]/table/tbody/tr[4]/td/a')
                index = 1
                for page in pages:
                    if page.text == u'下一页':
                        site = page.get_attribute('href')
                        logger.info(u'------第%d页------',self.page_index)
                        # print '------第%d页------'%self.page_index
                        self.page_index = self.page_index + 1
                        by_xpath = '/html/body/table[9]/tbody/tr/td[2]/table/tbody/tr[4]/td/a[%d]'%index
                        driver.find_element_by_xpath(by_xpath).click()
                        flag = True
                    else:
                        flag = False
                    index = index + 1
                # print flag

            except Exception,e:
                    logger.info('error:%s',e)
                    # print Exception, ":",e
                    # traceback.print_exc()

        self.sqlutil.disconnect()
        logger.info(u'关闭数据库连接')
        return items


if __name__ == '__main__':
    scrapy.cmdline.execute(argv=['scrapy', 'crawl', 'henan'])
