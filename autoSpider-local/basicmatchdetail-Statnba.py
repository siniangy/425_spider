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
collection = db['basicmatchdetail']

'''
date:2019/4/10

1：爬取2019年的NBA比赛常规统计数据及比赛信息写进mongoDB
2：schedule每周三的22:00爬取
3：以2019-03-01公牛对老鹰为例
url： http://stat-nba.com/game/43526.html
db：match
collection：matchdetail
json = {
    'date':'xxxx-xx-xx',
    'url':'xxxx.html',
    'team1Info':['name','img','赛前战绩(xx-xx)'],
    'team1Home':['主客队'],
    'team1Score':['第一节得分','第二节得分','第三节得分','第四节得分'],
    'team2Info':['name','img','赛前战绩(xx-xx)'],
    'team1Home':['主客队'],
    'team2Score':['第一节得分','第二节得分','第三节得分','第四节得分'],
    'team1Detail':[[xx1Detail],[xx2Detail]...] ,
    'team1Summary':[[team1Summary]],
    'team2Detail':[[xx1Detail],[xx2Detail]...],
    'team1Summary':[team1Summary]
}
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
def parse_match(html,url):
    soup = BeautifulSoup(html,'lxml')
    basic = soup.find('div','basic')
    Statbox = soup.find_all('div','stat_box')
    if Statbox:
        i=0
        target={}
        target[u'url'] = str(url)
        for team in basic.find_all('div','team'):
            i+=1
            dataInfo=[]
            for a in team.find_all('a',href=re.compile('/team.*?.html')):
                if(a.text.strip() == ''):
                    dataInfo.append('http://stat-nba.com/' + a.find('img')['src'])
                else:
                    dataInfo.append(a.text)
            dataInfo.append(team.find('a',href=re.compile('/query.*')).text)
            target[u'team'+str(i)+'Info'] = dataInfo
        i=0
        Score = basic.find('div','scorebox')
        for text in Score.find_all('div','text'):
            i+=1
            dataHome=[]
            for div in text.find_all('div'):
                dataHome.append(div.text)
            target[u'team'+str(i)+'Home']=dataHome
        i=0
        for table in Score.find_all('div','table'):
            i+=1
            dataScore=[]
            for td in table.find_all('td','number'):
                dataScore.append(td.text)
            target[u'team'+str(i)+'Score'] = dataScore
        i=0
        for statbox in Statbox:
            DataDetail=[]
            DataSummary=[]
            i+=1
            for tr in statbox.find_all('tr','sort'):
                d=[]
                for td in tr.find_all('td'):
                    if td.text.strip() != '':
                        d.append(str(td.text))
                    else:
                        continue
                DataDetail.append(d)
            if(DataDetail != []):
                target[u'team'+str(i)+'Detail'] = DataDetail
            for tr in statbox.find_all('tr','team_all_content'):
                d=[]
                for td in tr.find_all('td'):
                    if(td.text != '-'):
                        d.append(str(td.text))
                    else:
                        continue
                DataSummary.append(d)
            if(DataSummary != []):
                target[u'team'+str(i)+'Summary'] = DataSummary

        # mongodb中不存在插入,这里不需更新
        if collection.find({'url': str(target['url'])}).count() == 0:
            save_to_mongo(target)
        else:
            pass
    else:
        pass

# MongoDB存储
def save_to_mongo(result):
    collection.insert_one(result)

# 工作
def job(url):
    html = get_one_page(url)
    if html:
        parse_match(html,url)
    else:
        pass
# 启动入口
def main():
    global m
    # for i in range (43078,43862): # 4月10号之前的比赛
    #     job('http://stat-nba.com/game/'+str(i)+'.html')
    #     print('比赛编号%s爬取完成'%(str(i)))
    #     time.sleep(1)

    # for i in range (43862,m):
    #     job('http://stat-nba.com/game/'+str(i)+'.html')
    #     print('比赛编号%s爬取完成' % (str(i)))
    #     time.sleep(1)

# 4月10号以后的比赛手动改变m
if __name__ == '__main__':
    main()