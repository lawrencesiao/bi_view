import csv
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# TODO:
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def crawl(start, end, target='Elephants'):
    exception1 = []
    exception2 = []
    posts = []
    replies = []
    nb = 0
    for q in range(start, end):
        try:
            res = requests.get('https://www.ptt.cc/bbs/{}/index{}.html'.format(target, q))
            res.encoding = 'utf-8'
            soup = BeautifulSoup(res.text, 'html.parser')
            for item in soup.select('.r-ent'):
                nb += 1
                if not len(item.select('a')):
                    continue
                
                res11 = 'https://www.ptt.cc' + item.select('a')[0]['href']
                res1 = requests.get(res11)
                res1.encoding='utf-8'
                soup1 = BeautifulSoup(res1.text, 'html.parser')
                postId = nb
                tag = 'Post'
                title = item.select('a')[0].text
                author = item.select('.author')[0].text

                try:
                    if soup1.find_all('span','article-meta-value')==[]:
                        postTime = datetime.strptime(soup1.find_all('span','f2')[2].text.split(", ")[1].replace("\n",""), '%m/%d/%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        if len(soup1.find_all('span','article-meta-value')[-1].text[4:])==15:
                            datetime.strptime(soup1.find_all('span','article-meta-value')[-1].text[4:] + ' 2016','%b  %d %H:%M:%S %Y')
                        else:
                            postTime = datetime.strptime(soup1.find_all('span','article-meta-value')[-1].text[4:],'%b  %d %H:%M:%S %Y').strftime('%Y-%m-%d %H:%M:%S %Y')
                except:
                    postTime=" "

                post = [postId,tag,title,author,postTime]
                posts.append(post)

                for push in soup1.findAll('div', {'class':'push'} ):
                    if not len(push.findAll('span')):
                        continue
                    tag = push.findAll('span')[0].text
                    author = push.findAll('span')[1].text
                    postTime = push.findAll('span')[3].text.replace("\n","")[-11:]
                    contend = push.findAll('span')[2].text[1:]
                    contend = contend.replace('\n','')
                    reply = [postId, tag, title,author,postTime,contend]
                    replies.append(reply)
        except:
            exception1.append(res11)

    return posts, replies
