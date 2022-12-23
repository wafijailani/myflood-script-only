from requests_html import HTMLSession
from cgitb import text
import requests
from bs4 import BeautifulSoup
session = HTMLSession()
import re
url = 'https://www.bernama.com/en/search.php?cat1=all&terms=flood&submit=search'

r = requests.get(url)
soup = BeautifulSoup(r.text,'html.parser')
# print(soup)

geturl = soup.find_all(class_ = 'row')[1]
#print(geturl)
x = []
for link in geturl.findAll('a', attrs={'href': re.compile('^https://www.bernama.com/en/general/news.php')}):
    link = link.get('href')
    print(link)
    x.append(link)
#geturl = soup.find_all(class_ = 'row')[1]

#print(x)

for i in x:
    print(i)