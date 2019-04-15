# coding=UTF-8
from urllib import request,error
import re
import time
import schedule
from bs4 import BeautifulSoup
from pymongo import MongoClient
from selenium import webdriver

# client = MongoClient('mongodb://127.0.0.1:27017/') # 阿里云
# client = MongoClient('mongodb://xxxx:27017/') # 阿里云
db = client['match']
collection = db['matchplaybyplaycn']

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
browser = webdriver.Chrome(options=chrome_options)

'''
date:2019/4/10

1：爬取2018-至今的NBA比赛时序数据写进mongoDB,数据来源stat-nba.
2：schedule每周二的22:00爬取
3：以2019-03-01公牛对老鹰为例
db: match
collection: matchplaybyplaycn
json = {
    'url': 'http://www.stat-nba.com/game/43526.html',
    'quarter1': [['时间','team1事件详情','team1事件总结','当前比分','team2事件总结','team2事件详情'],...],
    'quarter2': [['时间','team1事件详情','team1事件总结','当前比分','team2事件总结','team2事件详情'],...],
    'quarter3': [['时间','team1事件详情','team1事件总结','当前比分','team2事件总结','team2事件详情'],...],
    'quarter4': [['时间','team1事件详情','team1事件总结','当前比分','team2事件总结','team2事件详情'],...]
}
'''

# 爬取页面
def get_one_page(url):
    try:
        browser.get(url)
        # 做一个判断
        soup = BeautifulSoup(browser.page_source, 'lxml')
        tbody = soup.find('tbody', id='detail_q')
        if tbody:
            target = {}
            target[u'url'] = url
            j = 0
            for i in ['.pbp_1q.pbp_chooser','.pbp_2q.pbp_chooser','.pbp_3q.pbp_chooser','.pbp_4q.pbp_chooser']:
                j += 1
                res = browser.find_element_by_css_selector(i)
                res.click()
                play = parse_match(browser.page_source)
                target[u'quarter'+str(j)] = play
            # print(target)
            # mongodb中不存在插入,这里不需更新
            if collection.find({'url': str(target['url'])}).count() == 0:
                save_to_mongo(target)
            else:
                pass
        else:
            pass
    finally:
        pass
# 解析页面
def parse_match(html):
    soup = BeautifulSoup(html,'lxml')
    tbody = soup.find('tbody',id='detail_q')
    play = []
    for tr in tbody.find_all('tr'):
        d = []
        for td in tr.find_all('td'):
            d.append(td.text)
        play.append(d)
    return play

# mongoDB存储
def save_to_mongo(result):
    collection.insert_one(result)

# 工作
def job(url):
    get_one_page(url)

# 启动
global m
m = 43848 # (截至到2019/4/9)
def main():
    global m
    m+=100
    # for i in range (43526,43581): # 测试了19年3月第一周的7天数据
    #     job('http://stat-nba.com/game/'+str(i)+'.html')
    #     print('比赛编号%s爬取完成' % (str(i)))
    #     time.sleep(1)
    # browser.close()

    # for i in range (43848,m):
    #     job('http://stat-nba.com/game/'+str(i)+'.html')
    #     print('比赛编号%s爬取完成' % (str(i)))
    #     time.sleep(1)
    # browser.close()

# if __name__ == '__main__':
#     main()

# 启动schedule定时爬取,一次启动多查询100场比赛，可以覆盖了(一周多100，一年就多查5000，垃圾,还不如直接清空再insert！！！)
# 定时爬取需要linux安装chromedriver！！
schedule.every().tuesday.at("22:00").do(main)
while True:
    schedule.run_pending()