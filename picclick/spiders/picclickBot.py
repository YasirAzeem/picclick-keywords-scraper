import scrapy
from bs4 import BeautifulSoup
import mysql.connector
import json
import datetime
from slugify import slugify


class PicclickbotSpider(scrapy.Spider):
    name = 'picclickBot'
    allowed_domains = ['picclick.com']
    start_urls = ['https://picclick.com/popular.html']
    conn = mysql.connector.connect(host='154.38.160.70',
                                   database='sql_usedpick_com',
                                   user='sql_usedpick_com',
                                   password='e5empmmWBjBEr5s6')
    cursor = conn.cursor(buffered=True)
    proxy = 'http://user-usabot:usabot@gate.dc.smartproxy.com:20000'

    def parse(self, response):
        soup = BeautifulSoup(response.body,'lxml')
        all_pop = [x for x in soup.find_all('a') if "https://picclick.com/Popular/" in x.get('href')]
        all_kws = {}
        print(f'Found {len(all_pop)} keywords')
        for a in all_pop:
            all_kws[a.text] = a.get('href')
            try:
                sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.all_keywords (keyword, slug) VALUES (%s, %s);'''
                self.cursor.execute(sql_update_query,(a.text,slugify(a.text)))
                self.conn.commit()
            except:
                self.conn = mysql.connector.connect(host='154.38.160.70',
                                   database='sql_usedpick_com',
                                   user='sql_usedpick_com',
                                   password='e5empmmWBjBEr5s6')
                self.cursor = self.conn.cursor(buffered=True)
                sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.all_keywords (keyword, slug) VALUES (%s, %s);'''
                self.cursor.execute(sql_update_query,(a.text,slugify(a.text)))
                self.conn.commit()
                
            yield scrapy.Request(url=a.get('href'), callback=self.parse2,meta={'kw':a.text.strip(),'crawl_once':True})
        return
        
        

    def parse2(self, response):
        soup = BeautifulSoup(response.body,'lxml')
 
        tree_1 = ['<li'+x.text.split('<li')[-1].split('</li>')[0]+'</li>' for x in soup.find_all('script') if "/Popular/" in x.text]
        if tree_1:
            for t in tree_1:
                tree_soup  = BeautifulSoup(t,'lxml')
                kw = tree_soup.find('a').text.strip()
                url = tree_soup.find('a').get('href')
                try:
                    sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.all_keywords (keyword, slug) VALUES (%s, %s);'''
                    self.cursor.execute(sql_update_query,(kw, slugify(kw)))
                    self.conn.commit()
                except:
                    self.conn = mysql.connector.connect(host='154.38.160.70',
                                   database='sql_usedpick_com',
                                   user='sql_usedpick_com',
                                   password='e5empmmWBjBEr5s6')
                    self.cursor = self.conn.cursor(buffered=True)
                    sql_update_query = f'''INSERT IGNORE INTO sql_usedpick_com.all_keywords (keyword, slug) VALUES (%s, %s);'''
                    self.cursor.execute(sql_update_query,(kw, slugify(kw)))
                    self.conn.commit()
                yield scrapy.Request(url="https://www.picclick.com"+url,callback=self.parse2, meta={'kw':kw,'crawl_once':True})

