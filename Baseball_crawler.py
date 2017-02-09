import requests
import csv
from datetime import datetime

from bs4 import BeautifulSoup
import pandas as pd

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

exception1=[]
exception2=[]
posts=[]
replies=[]
nb=0
for q in range(3410,3415):
    print q
    try:
        res = requests.get('https://www.ptt.cc/bbs/Elephants/index'+str(q)+'.html')
        res.encoding='utf-8'
        soup = BeautifulSoup(res.text)
        for item in soup.select('.r-ent'):
            nb = nb+1
            if len(item.select('a'))==0:
                continue
            else:
                res11 = 'https://www.ptt.cc' + item.select('a')[0]['href']
            res1 = requests.get(res11)
            res1.encoding='utf-8'
            soup1 = BeautifulSoup(res1.text)
            postId = nb
            tag = 'Post'
            title = item.select('a')[0].text
        
        
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
                if len(push.findAll('span'))==0:
                    pass
                else:    
                    tag = push.findAll('span')[0].text
                    author = push.findAll('span')[1].text
                    postTime = push.findAll('span')[3].text.replace("\n","")[-11:]
                    contend = push.findAll('span')[2].text[1:]
                    contend = contend.replace('\n','')
                    reply = [postId, tag, title,author,postTime,contend]
                    replies.append(reply)
    
    except:
        print 'exception1'
        exception1.append(res11)
        pass
    
    res = requests.get('https://www.ptt.cc/bbs/baseball/index'+str(q)+'.html')
    soup = BeautifulSoup(res.text)
    for item in soup.select('.r-ent'):
        try:
            nb = nb+1
            res12 = 'https://www.ptt.cc' + item.select('a')[0]['href']
            res1 = requests.get(res12)
            soup1 = BeautifulSoup(res1.text)
            postId = nb
            tag = 'Post'
            title = item.select('a')[0].text
        
            
            title = item.select('a')[0].text
            author = item.select('.author')[0].text
            if soup1.find_all('span','article-meta-value')==[]:
                postTime = ''
            else:
                postTime = soup1.find_all('span','article-meta-value')[-1].text

        
        
            for push in soup1.findAll('div', {'class':'push'} ):
                if len(push.findAll('span'))==0:
                    pass
                else:    
                    tag = push.findAll('span')[0].text
                    author = push.findAll('span')[1].text
                    postTime = push.findAll('span')[3].text.replace("\n","")[-11:]
                    contend = push.findAll('span')[2].text[1:]
                    contend = contend.replace('\n','')
                    reply = [postId, tag, title,author,postTime,contend]
                    replies.append(reply)
        except:
            print 'exception2'
            exception2.append(res12)
            pass
                
f = open("replies_0426.csv","w")  
w = csv.writer(f) 
w.writerows(replies)  
f.close()  


f = open("posts_0426.csv","w")  
w = csv.writer(f) 
w.writerows(posts)  
f.close()  


