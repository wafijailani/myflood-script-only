from cgitb import text
from bs4 import BeautifulSoup
import requests
import pandas as pd
import mysql.connector
import numpy as np
from pandas.io import sql 
import MySQLdb
from sqlalchemy import create_engine


from twilio.rest import Client 
account_sid = 'ACb021c571485ced66b9bc93264c6f7123' 
auth_token = 'c8724f44099719abe96e562cecfdfa15' 
client = Client(account_sid, auth_token) 

# Step 1: Sending a HTTP request to a URL
url = "http://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data?state=WLH&district=ALL&station=ALL&lang=en" # for Wilayah Perseketuan KL 
#url = "http://publicinfobanjir.water.gov.my/aras-air/data-paras-air/aras-air-data?state=SEL&district=ALL&station=ALL&lang=en" #for Selangor
r = requests.get(url)
soup = BeautifulSoup(r.text, 'html.parser')
#print(soup)

df = pd.DataFrame(columns = ["Station ID","Station Name","District","Main Basin (mm)","Sub River Basin (mm)",
"Last Update","CurrentWaterLevel","Normal","Alert","Warning","Danger"])

water2 = soup.find('table', class_ = 'normaltable-fluid')
print(water2)
for id in water2.find_all('tbody'):
	rows = id.find_all('tr', class_ = 'item')
	#print(rows) #extract the exact table only in html format
	for row in rows:
		statID = row.find('td', attrs={'data-th':'Station ID'}).text
		#print(statID)
		statname = row.find('td', attrs={'data-th':'Station Name'}).text
		#print(statname)
		dist = row.find('td', attrs={'data-th':'District'}).text
		#print(dist)
		mainbas = row.find('td', attrs={'data-th':'Main Basin (mm)'}).text
		#print(mainbas)
		subbase = row.find('td', attrs={'data-th':'Sub River Basin (mm)'}).text
		#print(subbase)
		lastup = row.find('td', attrs={'data-th':'Last Update'}).text
		#print(lastup)
		currentlevel = row.find('td', attrs={'data-th':'wl'}).text
		#print(currentlevel)
		normal = row.find('td', attrs={'data-th':'Normal'}).text
		#print(normal)
		alert = row.find('td', attrs={'data-th':'Alert'}).text
		#print(alert)
		warning = row.find('td', attrs={'data-th':'Warning'}).text
		#print(warning)
		danger = row.find('td', attrs={'data-th':'Danger'}).text
		#print(danger)
		addrow = {"Station ID":statID,"Station Name":statname,"District":dist,"Main Basin (mm)":mainbas,
		"Sub River Basin (mm)":subbase,"Last Update":lastup,"CurrentWaterLevel":currentlevel,
		"Normal":normal,"Alert":alert,"Warning":warning,"Danger":danger}
		df = df.append(addrow, ignore_index=True)

#message = client.messages.create(messaging_service_sid='MG2815ed8745527fe1d9366bdf1cbbfc4d', body='Hi There',to='+601111184278') # send sms api 
#def f(x):    
#   return 'hello' if x['CurrentWaterLevel'] >= x['Alert'] else 'no' 
#print(df.apply(f, axis=1))


#mydb = mysql.connector.connect(user='root', password='',host='localhost', database='website_myflood')
#print(mydb)
#mydbcursor = mydb.cursor()
#mydbcursor.execute("SELECT phonenum FROM users")
#result = mydbcursor.fetchall()

#for phonenumber in result:
#	message = client.messages.create(messaging_service_sid='MG2815ed8745527fe1d9366bdf1cbbfc4d', body='Flood Alert',to=phonenumber) # send sms api
#	message = client.messages.create(messaging_service_sid='MG2815ed8745527fe1d9366bdf1cbbfc4d', body='Flood Warning',to=phonenumber) # send sms api
#	message = client.messages.create(messaging_service_sid='MG2815ed8745527fe1d9366bdf1cbbfc4d', body='Flood Danger',to=phonenumber) # send sms api




#if empty create 
# else update
print(df.dtypes)

convert_dict = {'CurrentWaterLevel': float, 'Normal':float,'Warning':float,
'Danger':float, 'Alert':float}	
df = df.astype(convert_dict)
print(df.dtypes)
#df.at[0,'CurrentWaterLevel']=96.6 testing purpose

df['smsAlert'] = np.where(df['CurrentWaterLevel']>df['Alert'], 'sms', 'nosms')
df['smsWarning'] = np.where(df['CurrentWaterLevel']>df['Warning'], 'sms', 'nosms')
df['smsDanger'] = np.where(df['CurrentWaterLevel']>df['Danger'], 'sms', 'nosms')


with pd.option_context('display.max_rows', None, 'display.max_columns', None):
	print(df)

#   return 'hello' if x['CurrentWaterLevel'] >= x['Alert'] else 'no' 
#print(df.apply(f, axis=1))

#df.to_sql('users', con=mydb)
con = MySQLdb.connect(user='root', password='',host='localhost', database='website_myflood')
concursor = con.cursor()



engine = create_engine("mysql+pymysql://root:@localhost/website_myflood")
df.to_sql('klscrape', con = engine, if_exists='replace')




for i, row in df.iterrows():
	if row['smsAlert'] == 'sms':
		x = df['District'][i]
		newformat = ("{}\n{}\n{}\n{}\n{}".format('Alert Warning from Myflood',df['Station Name'][i], df['CurrentWaterLevel'][i], df['Last Update'][i],'Check Website for More Information'))
		concursor.execute("SELECT phonenum FROM users WHERE district = '{x}'".format(x=x))
		result = concursor.fetchall()
		print(result)
		for i in range(len(result)):
			message = client.messages.create(messaging_service_sid='MG2815ed8745527fe1d9366bdf1cbbfc4d', body=newformat ,to=result[i])

for i, row in df.iterrows():
	if row['smsWarning'] == 'sms':
		x = df['District'][i]
		newformat = ("{}\n{}\n{}\n{}\n{}".format('Alert Warning from Myflood',df['Station Name'][i], df['CurrentWaterLevel'][i], df['Last Update'][i],'Check Website for More Information'))
		concursor.execute("SELECT phonenum FROM users WHERE district = '{x}'".format(x=x))
		result = concursor.fetchall()
		print(result)
		for i in range(len(result)):
			message = client.messages.create(messaging_service_sid='MG2815ed8745527fe1d9366bdf1cbbfc4d', body=newformat ,to=result[i])

for i, row in df.iterrows():
	if row['smsDanger'] == 'sms':
		x = df['District'][i]
		newformat = ("{}\n{}\n{}\n{}\n{}".format('Alert Warning from Myflood',df['Station Name'][i], df['CurrentWaterLevel'][i], df['Last Update'][i],'Check Website for More Information'))
		concursor.execute("SELECT phonenum FROM users WHERE district = '{x}'".format(x=x))
		result = concursor.fetchall()
		print(result)
		for i in range(len(result)):
			message = client.messages.create(messaging_service_sid='MG2815ed8745527fe1d9366bdf1cbbfc4d', body=newformat ,to=result[i])

# for i, row in df.iterrows():
#     if row['smsWarning'] == 'sms':
#         mesej = (df.iat[i,1])
#         print(mesej)
#         message = client.messages.create(messaging_service_sid='MG2815ed8745527fe1d9366bdf1cbbfc4d', body=mesej,to=+601111184278)

# for i, row in df.iterrows():
#     if row['smsDanger'] == 'sms':
#         mesej = (df.iat[i,1])
#         print(mesej)
#         message = client.messages.create(messaging_service_sid='MG2815ed8745527fe1d9366bdf1cbbfc4d', body=mesej,to=+601111184278)	