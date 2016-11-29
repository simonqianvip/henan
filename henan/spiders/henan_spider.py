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

    download_count = 0

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
        s_time = self.GetNowTime()

        driver = webdriver.PhantomJS()
        driver.get(response.url)
        flag = True

        items = []

        try:
            while(flag):
                # TODO 获取主页面的handle
                current_window_handle = driver.current_window_handle

                tr_tags = driver.find_elements_by_xpath('/html/body/table[9]/tbody/tr/td[2]/table/tbody/tr[3]/td/table/tbody/tr')
                for i in range(1,tr_tags.__len__()+1):

                    # 获取标题和link
                    # a_xpath = '/html/body/table[9]/tbody/tr/td[2]/table/tbody/tr[3]/td/table/tbody/tr[%d]/td/table/tbody/tr/td[2]/a'%i
                    a_xpath = '/html/body/table[9]/tbody/tr/td[2]/table/tbody/tr[3]/td/table/tbody/tr[%d]/td/table/tbody/tr/td[2]'%i
                    a_tag = driver.find_element_by_xpath(a_xpath)
                    title = a_tag.text
                    # 根据text定位标签
                    link_text = driver.find_element_by_link_text(title)
                    link = link_text.get_attribute('href')
                    # logger.info('href = %s',link)

                    # 获取日期
                    td_xpath = '/html/body/table[9]/tbody/tr/td[2]/table/tbody/tr[3]/td/table/tbody/tr[%d]/td/table/tbody/tr/td[3]'%i
                    td_tag = driver.find_element_by_xpath(td_xpath)
                    date = td_tag.text
                    logger.info('title = %s ,date = %s ,href = %s '%(title,date,link))

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

                            for h in handles:
                                if h != current_window_handle:
                                    driver.switch_to_window(h)

                            # print driver.page_source
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
                            self.download_count = self.download_count + 1

                            item = HenanItem()
                            item['title'] = title
                            item['link'] = link
                            item['date'] = date
                            item['titleId'] = titleId
                            items.append(item)
                            # 关闭当前二级页面
                            second_all_handle = driver.window_handles
                            for s in second_all_handle:
                                if s != current_window_handle:
                                    driver.switch_to_window(s)
                                    driver.close()
                            # 切回到主界面
                            driver.switch_to_window(current_window_handle)

                flag = self.next_page(driver, flag)

            # TODO 爬虫数据更新
            e_time = self.GetNowTime()
            # 减掉公司概况的一条数据
            u_count = len(items)
            # 插入更新语句
            insert_sql = """
                    insert into spider_info(site,s_time,e_time,update_count,download_count)
                    values('河南政府政策信息网','%s','%s','%s','%s')""" % (s_time, e_time, u_count, self.download_count)
            self.sqlutil.exec_db_cmd(insert_sql)

        except Exception,e:
                logger.error(e)
        finally:
            driver.close()
            self.sqlutil.disconnect()
            logger.info(u'关闭数据库连接')
            return items

    def next_page(self, driver, flag):
        """
        点击下一页
        :param driver:
        :param flag:
        :return:
        """
        pages = driver.find_elements_by_xpath('/html/body/table[9]/tbody/tr/td[2]/table/tbody/tr[4]/td/a')
        index = 1
        for page in pages:
            if page.text == u'下一页':
                site = page.get_attribute('href')
                logger.info(u'------第%d页------', self.page_index)
                self.page_index = self.page_index + 1
                by_xpath = '/html/body/table[9]/tbody/tr/td[2]/table/tbody/tr[4]/td/a[%d]' % index
                driver.find_element_by_xpath(by_xpath).click()
                flag = True
            else:
                flag = False
            index = index + 1
        return flag

    def GetNowTime(self):
        """
        获取当前系统时间
        :return:
        """
        return time.strftime("%Y-%m-%d %H:%M:%S")


if __name__ == '__main__':
    scrapy.cmdline.execute(argv=['scrapy', 'crawl', 'henan'])
