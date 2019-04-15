# coding=UTF-8
from urllib import request,error
import time
import schedule
from bs4 import BeautifulSoup
from pymongo import MongoClient

client = MongoClient('mongodb://127.0.0.1:27017/') # 阿里云
db = client['match']
collection = db['playeraveragedata']


'''
date:2019/4/10

1：爬取NBA所有球员（包括退役的和在役的）的场均数据写入MongoDB（场均数据只处理常规赛）
2：迭代处理0-5000名球员(一年内应该不会变动)，scedule每周三22:00爬取
4：全量更新，直接drop再insert(数据更新频繁，不做判断)
5：以勒布朗-詹姆斯为例
url: http://www.stat-nba.com/player/1862.html
db: matchlist
collection: matchplayer
json = {
    'url': 'http://www.stat-nba.com/player/1862.html',
    'cnName': '勒布朗-詹姆斯',
    'engName': 'LeBron James',
    'img': 'http://www.stat-nba.com/image/playerImage/1862.jpg',
    'baidu': 'http://baike.baidu.com/view/110186.htm',
    'wiki': 'http://en.wikipedia.org/wiki/LeBron_James'
    'seasonAvg': ["16年",  NBA生涯
        "3支",  效力球队
        "1177", 出场
        "1176", 首发
        "38.6", 时间
        "50.4%", 投篮命中率
        "9.9", 命中数
        "19.6", 出手数
        "34.4%", 三分命中率
        "1.4", 三分命中数
        "4.2", 三分出手数
        "73.8%", 罚球命中率
        "6.0", 罚球命中数
        "8.1", 罚球出手数
        "7.4", 篮板
        "1.2", 前场篮板
        "6.2", 后场篮板
        "7.2", 助攻
        "1.6", 抢断
        "0.8", 盖帽
        "3.5", 失误
        "1.8", 犯规
        "27.2", 得分
        "781", 胜场
        "396"], 负场 
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
    # (考虑情况作过滤)
    soup = BeautifulSoup(html,'lxml')
    info = soup.find('div',class_='playerinfo')
    target = {}
    target[u'url'] = url
    # name过滤
    name = info.find('div',class_='name').text.split('\n')[0].split('/')
    if name:
        if len(name) == 1:
            target[u'engName'] = name[0]
        if len(name) == 2:
            target[u'cnName'] = name[0]
            target[u'engName'] = name[1]
    target[u'img'] = 'http://www.stat-nba.com'+str(info.find('img')['src'])
    # baidu和wiki百科过滤
    baike = info.find('div',class_='name').find_all('a')
    if baike:
        if len(baike) == 1:
            target[u'wiki'] = baike[0]['href']
        if len(baike) == 2:
            target[u'baidu'] = baike[0]['href']
            target[u'wiki'] = baike[1]['href']
    stat = soup.find('div',id='stat_box')
    if stat:
        for d in stat.find_all('div'):
            if d.text.strip() != '':
                table = d.find('table',id='stat_box_avg')
                if table:
                    content = table.find('tbody').find_all('tr')[-1].text.split('\n')
                    while '' in content:
                        content.remove('')
                    target[u'seasonAvg'] = content
                else:
                    continue
            else:
                continue
    # print(target)
    save_to_mongo(target)

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

# 启动
def main():
    collection.drop()
    for i in range(1,5001):
        job('http://www.stat-nba.com/player/'+str(i)+'.html')
        print('比赛编号%s爬取完成' % (str(i)))
        time.sleep(1)

# if __name__ == '__main__':
#     main()

# 启动schedule定时爬取
schedule.every().wednesday.at("22:00").do(main)
while True:
    schedule.run_pending()