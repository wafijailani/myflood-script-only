from cgitb import text
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import MySQLdb
import re
from sqlalchemy import create_engine

df = pd.DataFrame(columns = ["Title","PictureLink","News"])

pages = ['1','2','3']
for i in range(0,len(pages)):
    url = 'https://www.bernama.com/en/search.php?cat1=all&terms=flood&page=' + pages[i]

    r = requests.get(url)
    soup = BeautifulSoup(r.text,'html.parser')

    geturl = soup.find_all(class_ = 'row')[1]

    x = []
    for link in geturl.findAll('a', attrs={'href': re.compile('^https://www.bernama.com/en/general/news.php')}):
        link = link.get('href')
        print(link)
        x.append(link)

    for i in x:

        url = i
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')

        title = soup.find(class_="h2").text
        print(title,'\n')

        image = soup.find("img", class_ = "lazyload")
        image = (image['data-src'])
        print(image)

        p1 = soup.find_all(class_="col-12 mt-3 text-dark text-justify")[0]
        p1 = p1.text
        #print(p1)

        news = soup.find_all(class_="col-12 mt-3 text-dark text-justify")[0]
        x = ''
        for e in news.find_all({'p'}):
            if e.span:
                _ = e.span.extract()
            x += e.text
        #print(x)

        news = soup.find_all(class_="col-12 mt-3 text-dark text-justify")[1]
        s = ''
        for e in news.find_all({'p'}):
            if e.span:
                _ = e.span.extract()
            s += e.text
        #print(s)

        news = soup.find_all(class_="col-12 mt-3 text-dark text-justify")[2]
        a = ''
        for e in news.find_all({'p'}):
            if e.span:
                _ = e.span.extract()
            a += e.text
        #print(a)

        newformat = ("{}\n{}\n{}".format(x,s,a))
        print(newformat)

        addrow = {"Title":title,"PictureLink":image,"News":newformat}
        df = df.append(addrow,ignore_index=True)

df['id'] = df.index
df = df[['id', 'Title', 'PictureLink', 'News']]

#print(df)
# con = MySQLdb.connect(user='root', password='',host='localhost', database='website_myflood')
# engine = create_engine("mysql+pymysql://root:@localhost/website_myflood")
# df.to_sql('newscrape', con = engine, if_exists='replace', index=False)

con = MySQLdb.connect(user='root', password='',host='localhost', database='myfloodlaravel')
engine = create_engine("mysql+pymysql://root:@localhost/myfloodlaravel")
df.to_sql('newscrapes', con = engine, if_exists='replace', index=False)
