# coding=UTF-8
from urllib import request,error
import re
import time
import schedule
from bs4 import BeautifulSoup
from pymongo import MongoClient
from selenium import webdriver

# client = MongoClient('mongodb://127.0.0.1:27017/') # 本地
client = MongoClient('mongodb://xxxx:27017/') # 阿里云
db = client['match']
collection = db['matchplaybyplaycn']

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless')
browser = webdriver.Chrome(options=chrome_options)

'''
date:2019/4/10

1：爬取2019-至今的NBA比赛时序数据写进mongoDB,数据来源stat-nba.
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
def main():
    m = 43582
    # 爬取2019年3月第一周测试数据集
    # for i in range (43526,43581):
    #     job('http://stat-nba.com/game/'+str(i)+'.html')
    #     print('比赛编号%s爬取完成' % (str(i)))
    #     time.sleep(1)
    # browser.close()

    # for i in range (43581,m):
    #     job('http://stat-nba.com/game/'+str(i)+'.html')
    #     print('比赛编号%s爬取完成' % (str(i)))
    #     time.sleep(1)

# 手动改变m
if __name__ == '__main__':
    main()