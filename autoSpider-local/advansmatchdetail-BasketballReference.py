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
collection = db['advansmatchdetail']

'''
date:2019/4/11

1：爬取2019年的NBA比赛统计进阶数据写进mongoDB00
2：schedule每周四的22:00爬取（不知道是爬虫策略还是网络问题，总是断！！）
3：以2019-03-01公牛对老鹰为例
url：https://www.basketball-reference.com/boxscores/201903010ATL.html
db：match
collection：advansmatchdetail
json = {
    'url':'xxxx.html',
    'team1Detail':[[xx1Detail],[xx2Detail]...] ,
    'team1Summary':[[team1Summary]],
    'team2Detail':[[xx1Detail],[xx2Detail]...],
    'team2Summary':[team2Summary]
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
def parse_match(html,url,team):
    soup = BeautifulSoup(html,'lxml')
    str1 = str(team[0]).lower()
    str2 = str(team[1]).lower()
    if soup.find('div','section_content'):
        target = {}
        target[u'url'] = str(url)
        divAFlag = 'div_box_'+str1+'_advanced'
        tbodyAFlag = 'box_'+str1+'_advanced'
        advansA = soup.find('div',id=divAFlag)
        tbody = advansA.find('table',id=tbodyAFlag).find('tbody')
        tfoot = advansA.find('table',id=tbodyAFlag).find('tfoot')
        team1AdvansDetail = []
        team1AdvansSummary = []
        for tr in tbody.find_all("tr"):
            playerAdvans = []
            playerAdvans.append(tr.find('th').text)
            for td in tr.find_all('td'):
                if td.text.strip() != '':
                    playerAdvans.append(td.text)
                else:
                    playerAdvans.append(str(.0))
            team1AdvansDetail.append(playerAdvans)
        for tr in tfoot.find_all('tr'):
            teamAdvans = []
            teamAdvans.append(tr.find('th').text)
            for td in tr.find_all('td'):
                teamAdvans.append(td.text)
            team1AdvansSummary.append(teamAdvans)
        target[u'team1AdvansDetail'] = team1AdvansDetail
        target[u'team1AdvansSummary'] = team1AdvansSummary

        divBFlag = 'div_box_'+str2+'_advanced'
        tbodyBFlag = 'box_'+str2+'_advanced'
        advansB = soup.find('div', id=divBFlag)
        tbody = advansB.find('table', id=tbodyBFlag).find('tbody')
        tfoot = advansB.find('table', id=tbodyBFlag).find('tfoot')
        team2AdvansDetail = []
        team2AdvansSummary = []
        for tr in tbody.find_all("tr"):
            playerAdvans = []
            playerAdvans.append(tr.find('th').text)
            for td in tr.find_all('td'):
                if td.text.strip() != '':
                    playerAdvans.append(td.text)
                else:
                    playerAdvans.append(str(.0))
            team2AdvansDetail.append(playerAdvans)
        for tr in tfoot.find_all('tr'):
            teamAdvans = []
            teamAdvans.append(tr.find('th').text)
            for td in tr.find_all('td'):
                teamAdvans.append(td.text)
            team2AdvansSummary.append(teamAdvans)
        target[u'team2AdvansDetail'] = team2AdvansDetail
        target[u'team2AdvansSummary'] = team2AdvansSummary

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

# 获取比赛详情页面
def get_match_detail(url,team):
    html = get_one_page(url)
    if html:
        parse_match(html,url,team)
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
    # 解析日期页
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
    for team in target: # 主队t才是真正的url拼接参数
        for t in team:
            string = 'https://www.basketball-reference.com/boxscores/%s%s%s%s.html' %(year,month,day,t)
            get_match_detail(string,team)
            time.sleep(1)

# 入口
def main():
    date = time.strftime('%Y-%m-%d',time.localtime(time.time()))
    year = int(date.split('-')[0])
    month = int(date.split('-')[1])
    # 测试集3月第一周
    # for y in range(year, year+1):
    #     for m in range(month-1, month):
    #         for d in range(1,8):
    #             job('https://www.basketball-reference.com/boxscores/?month='+str(m)+'&day='+str(d)+'&year='+str(y),y,m,d)
    #             time.sleep(1)

# 手动改变日期，外网会断，这里不做定时爬取的测试！
if __name__ == '__main__':
    main()