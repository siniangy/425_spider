# coding=UTF-8
from urllib import request,error
import time
import schedule
from bs4 import BeautifulSoup
from pymongo import MongoClient

client = MongoClient('mongodb://127.0.0.1:27017/') # 阿里云
# client = MongoClient('mongodb://xxxx:27017/') # 阿里云
db = client['match']
collection = db['matchshotchart']

'''
date:2019/4/11

1：爬取2018年至今的NBA比赛投篮事件坐标数据写进mongodb
2：schedule每周四的22:00爬取
3：以2019-03-01公牛对老鹰为例
url：https://www.basketball-reference.com/boxscores/shot-chart/201903010ATL.html
db: match
collection: matchshotchart
json = {
    'url': 'xxxx.html',
    'team1Img': 'http://d2p3bygnnzw9w3.cloudfront.net/req/1/images/bbr/nbahalfcourt.png',
    'team2Img': 'http://d2p3bygnnzw9w3.cloudfront.net/req/1/images/bbr/nbahalfcourt.png',
    'team1ChartData':[['index'],['topX','leftY'],['content'],['result']],...],
    'team2ChartData':[['index'],['topX','leftY'],['content'],['result']],...]
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

# 解析比赛详情页
def parse_match(html,url):
    soup = BeautifulSoup(html,'lxml')
    target = {}
    target[u'url'] = url
    i= 0
    divs = soup.find_all('div',class_='shot-area')
    if divs:
        for div in divs:
            i+=1
            # img
            target[u'team'+str(i)+'Img'] = div.find('img')['src']
            # chartData
            target[u'team'+str(i)+'ChartData'] = []
            chartData = []
            j = 0
            for div in div.find_all('div'):
                d = []
                d.append(str(j))
                j+=1
                d.append(div['style'])
                d.append(div['tip'])
                d.append(div.text)
                chartData.append(d)
            target[u'team' + str(i) + 'ChartData'] = chartData
        # print(target)
        # 判断是否插入
        if collection.find({'url':str(url)}).count() == 0:
            print("比赛%s入库成功" %(str(url)))
            save_to_mongo(target)
        else:
            print("重复入库")
    else:
        pass

# MongoDB存储
def save_to_mongo(result):
    collection.insert_one(result)

# 获取比赛投篮坐标
def get_match_chart(url):
    html = get_one_page(url)
    if html:
        parse_match(html,url)
    else:
        pass

# 工作
def job(url,year,month,day):
    if day < 10:
        day = '0' + str(day) + '0'
    else:
        day = str(day) + '0'
    if month < 10:
        month = '0' + str(month)
    else:
        month = str(month)
    year = str(year)

    dict = {'Brooklyn':'BRK',
            'Orlando': 'ORL',
            'Boston': 'BOS',
            'San Antonio': 'SAS',
            'Cleveland': 'CLE',
            'Charlotte': 'CHO',
            'Detroit': 'DET',
            'LA Clippers': 'LAC',
            'Golden State': 'GSW',
            'Phoenix': 'PHO',
            'Houston': 'HOU',
            'Indiana': 'IND',
            'Utah': 'UTA',
            'LA Lakers': 'LAL',
            'Dallas': 'DAL',
            'Memphis': 'MEM',
            'Atlanta': 'ATL',
            'Milwaukee': 'MIL',
            'Oklahoma City': 'OKC',
            'Minnesota': 'MIN',
            'Washington': 'WAS',
            'New York': 'NYK',
            'Denver': 'DEN',
            'Portland': 'POR',
            'New Orleans': 'NOP',
            'Sacramento': 'SAC',
            'Miami': 'MIA',
            'Toronto': 'TOR',
            'Philadelphia': 'PHI',
            'Chicago': 'CHI'
            }
    target = []
    html = get_one_page(url)
    if html:
        soup = BeautifulSoup(html,'lxml')
        gameSummary = soup.find('div','game_summaries')
        if gameSummary:
            for div in gameSummary.find_all('div'):
                d = []
                d.append(dict[div.find('tr','winner').find('td').text])
                d.append(dict[div.find('tr','loser').find('td').text])
                target.append(d)
        else:
            target = []
    else:
        pass
    for team in target:
        for t in team:
            string = 'https://www.basketball-reference.com/boxscores/shot-chart/%s%s%s%s.html' %(year,month,day,t)
            get_match_chart(string)
            time.sleep(1)

# 入口
def main():
    # collection.drop();
    date = time.strftime('%Y-%m-%d',time.localtime(time.time()))
    year = int(date.split('-')[0])
    month = int(date.split('-')[1])
    # 2018年
    # for y in range(year-1, year):
    #     for m in range(1, 13):
    #         for d in range(1,31):
    #             job('https://www.basketball-reference.com/boxscores/?month='+str(m)+'&day='+str(d)+'&year='+str(y),y,m,d)
    # 2019年至今
    # for y in range(year, year+1):
    #     for m in range(month, month+1):
    #         for d in range(1,3):
    #             job('https://www.basketball-reference.com/boxscores/?month='+str(m)+'&day='+str(d)+'&year='+str(y),y,m,d)
    #             time.sleep(5)
    # 测试集3月第一周
    for y in range(year, year + 1):
        for m in range(month - 1, month):
            for d in range(8, 9):
                job('https://www.basketball-reference.com/boxscores/?month=' + str(m) + '&day=' + str(
                    d) + '&year=' + str(y), y, m, d)
                time.sleep(1)

# if __name__ == '__main__':
#     main()

# schedule启动（先爬取这一天的比赛获取url参数，再进入比赛详情页）（考虑到外网会断的问题，这里没有使用到定时爬取）
schedule.every().thursday.at("22:00").do(main)
while True:
    schedule.run_pending()