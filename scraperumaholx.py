# -*- coding: utf-8 -*-
#rachmat_sn
# scrapy runspider scrapeolx.py

import scrapy
import sqlite3

count = 0
page_url = []

#SQLITE3
dbname = 'jualrumahOLX.db'
conn = sqlite3.connect(dbname)
c = conn.cursor()

def create_table():
    c.execute("CREATE TABLE IF NOT EXISTS rumah(ad_id TEXT, img TEXT, txt TEXT, status TEXT, city TEXT, price INTEGER, UNIQUE(ad_id))")
    c.execute("DELETE FROM rumah")

#SCRAPY
def textBeautify(data):
    return list(map(lambda s: s.strip(), data))

def textBeautifyBrand(data):
    return list(map(lambda s: s.strip()[8:], data))

def rupiahToNumber(rupiah):
    noRp = rupiah[3:]
    noDot = noRp.replace(".", "")
    if noDot == '':
        return ''
    else:
        integer = int(noDot)
        return integer

def generate_page_url():
    numofpage = 250 #jumlah halaman yang di scrape
    for i in range(1,numofpage):
        if i==1:
            page_url.append('https://www.olx.co.id/properti/rumah/')
        else:
            page_url.append('https://www.olx.co.id/properti/rumah/?page='+str(i))
    return page_url
        
class ScrapeolxSpider(scrapy.Spider):
    name = 'scraperumaholx'
    create_table()
    page_url = generate_page_url()
    #print(page_url)
    
    def start_requests(self):
        for url in page_url:
            yield scrapy.Request(url=url, callback=self.parse)
    
    def parse(self, response):        
        print('test')
        ad_id = textBeautify(response.css('td.offer>table>tbody>tr::attr(data-ad-id)').extract())
        img = textBeautify(response.css('td.offer>table>tbody>tr>td>span>a>img.fleft::attr(src)').extract())
        txt = textBeautify(response.css('td.offer>table>tbody>tr>td>h2>a::text').extract())
        
        #brand################################
        ST = response.css('td.offer>table>tbody>tr>td>p>small.breadcrumb::text').extract() #brand
        status = []
        for i in range (0, len(ST),2):
            status.append(ST[i])
        status = textBeautifyBrand(status)
        city = textBeautify(response.css('td.offer>table>tbody>tr>td>p>small.breadcrumb>span::text').extract())
        price = textBeautify(response.css('td.offer>table>tbody>tr>td>div>p.price>strong::text').extract())    
        
#        print(ad_id)
#        print(img)
#        print(txt)
#        print(status)
#        print(city)
#        print(price)
        #cari yg panjangnya paling kecil untuk acuan
        jum_data_per_iter = min([len(ad_id), len(img), len(txt), len(status), len(city), len(price)])
        for it in range(jum_data_per_iter):
            scraped_info = {
                'ad_id': ad_id[it],
                'img': img[it],
                'txt': txt[it],
                'status': status[it],
                'city': city[it],
                'price': rupiahToNumber(price[it]), 
            }
            
            #commit jika semua data tidak kosong '' DAN brand bukan 'Lain-lain'
            if(scraped_info['ad_id']!='' and scraped_info['img']!='' and scraped_info['txt']!='' and scraped_info['status']!='' and scraped_info['city']!='' and scraped_info['price']!='' and scraped_info['status']!='Lain-lain'):
                c.execute("INSERT OR IGNORE INTO rumah (ad_id, img, txt, status, city, price) VALUES(?,?,?,?,?,?)",
                      (scraped_info['ad_id'],scraped_info['img'], scraped_info['txt'], scraped_info['status'], scraped_info['city'], scraped_info['price']))
                conn.commit()
                
            yield scraped_info