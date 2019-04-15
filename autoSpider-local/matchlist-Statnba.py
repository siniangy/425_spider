# coding=UTF-8
from urllib import request,error
import re
import time
import schedule
from bs4 import BeautifulSoup
from pymongo import MongoClient

client = MongoClient('mongodb://127.0.0.1:27017/') # 本地
# client = MongoClient('mongodb://xxxx:27017/') # 阿里云
db = client['match']
collection = db['matchlist']

'''
date:2019/4/10

1：爬取2019年NBA赛程写进mongoDB
2：scedule每周三22:00爬取
3：以2019年3月1号为例
db：match
collection：matchlist
json = {'date':'2019-03-01',
        'content':['公牛vs老鹰','黄蜂vs篮网'...],
        'url':[url1,url2,...]}
'''

# 爬取页面
def get_one_page(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.162 Safari/537.36'
    }
    try:
        req = request.Request(url=url,headers=headers)
        response = request.urlopen(req).read().decode('utf-8')
        return response
    except error.HTTPError as e:
        pass

# 解析页面
def parse_match(html):
    soup = BeautifulSoup(html,'lxml')
    cheight = soup.find_all('div','cheight')
    base_url='http://stat-nba.com/'
    for div in cheight:
        if div.text.strip()!='':
            d={}
            date=div.find('font','cheightdate')
            d[u'date']=str(date.text)
            matchs = []
            urls = []
            for match in div.find_all('a',href=re.compile('game/.*?html',re.S)):
                matchs.append(str(match.text))
                urls.append(base_url+str(match['href']))
            d[u'content']=matchs
            d[u'url']=urls

            # mongodb中不存在插入
            if collection.find({'date': str(date.text)}).count() == 0 :
                save_to_mongo(d)
            else:
                # mongodb中存在判断是否更新
                for i in collection.find({'date': str(date.text)}):
                    if i['content'] == d['content']:
                        pass # 内容不变化不更新
                    else:
                        collection.update({'date': str(date.text)},{'$set':{'content':d['content']}}) # 内容变化更新
                        print('内容更新')
        else:
            continue

# MongoDB存储
def save_to_mongo(result):
    collection.insert_one(result)

# 工作
def job(year,month):
    url = 'http://stat-nba.com/gameList_simple-'+str(year)+'-'+str(month)+'.html'
    html = get_one_page(url)
    if html:
        parse_match(html)
    else:
        pass

# 启动入口
def main():
    date = time.strftime('%Y-%m-%d',time.localtime(time.time()))
    year = int(date.split('-')[0])
    month = int(date.split('-')[1])

    # 2019年数据更新
    # for j in range(year, year+1):
    #     for i in range(month-1,month+1): # 前后一个月确定不漏数据,垃圾的判断方式
    #         if i < 10:
    #             i = '0' + str(i)
    #         else:
    #             i = str(i)
    #         job(year=str(j), month=str(i))
    #         print('爬取%s月MatchList数据完成' %(str(i)))
    #         time.sleep(1)

# 1-3月已经爬完，手动启动
if __name__ == '__main__':
    main()