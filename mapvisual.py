import pandas as pd
from pandas.io import sql 
import MySQLdb
from sqlalchemy import create_engine


con = MySQLdb.connect(user='root', password='',host='localhost', database='myfloodlaravel')
concursor = con.cursor()

df = pd.read_csv("latlongnewcombined.csv", names=['statid','lat','long']) 
df.dropna(inplace=True)


df['value'] = ''
df['alert'] = ''
df['warning'] = ''
df['danger'] = ''
df['stationname'] = ''
df['lastupdate'] = ''


for i in range(0,24):
   x = df['statid'][i]
   state = ("SELECT CurrentWaterLevel FROM `selangorscrape` WHERE `Station ID` = %(x)s")
   concursor.execute(state, {'x': x })
   result = concursor.fetchall()
   print(result)
   df['value'][i] = result
for i in range(0,24):
   x = df['statid'][i]
   state = ("SELECT Alert FROM `selangorscrape` WHERE `Station ID` = %(x)s")
   concursor.execute(state, {'x': x })
   result = concursor.fetchall()
   print(result)
   df['alert'][i] = result
for i in range(0,24):
   x = df['statid'][i]
   state = ("SELECT Warning FROM `selangorscrape` WHERE `Station ID` = %(x)s")
   concursor.execute(state, {'x': x })
   result = concursor.fetchall()
   print(result)
   df['warning'][i] = result
for i in range(0,24):
   x = df['statid'][i]
   state = ("SELECT Danger FROM `selangorscrape` WHERE `Station ID` = %(x)s")
   concursor.execute(state, {'x': x })
   result = concursor.fetchall()
   print(result)
   df['danger'][i] = result
for i in range(0,24):
   x = df['statid'][i]
   concursor.execute("SELECT `Station Name` FROM `selangorscrape` WHERE `Station ID` = '{x}'".format(x=x))
   result = concursor.fetchall()
   print(result)
   df['stationname'][i] = result

for i in range(0,24):
   x = df['statid'][i]
   state = ("SELECT `Last Update` FROM `selangorscrape` WHERE `Station ID` = %(x)s")
   concursor.execute(state, {'x': x })
   result = concursor.fetchall()
   print(result)
   df['lastupdate'][i] = result
    
#for kl 
for i in range(24,33):
   x = df['statid'][i]
   state = ("SELECT CurrentWaterLevel FROM `klscrape` WHERE `Station ID` = %(x)s")
   concursor.execute(state, {'x': x })
   result = concursor.fetchall()
   print(result)
   df['value'][i] = result
for i in range(24,33):
   x = df['statid'][i]
   state = ("SELECT Alert FROM `klscrape` WHERE `Station ID` = %(x)s")
   concursor.execute(state, {'x': x })
   result = concursor.fetchall()
   print(result)
   df['alert'][i] = result
for i in range(24,33):
   x = df['statid'][i]
   state = ("SELECT Warning FROM `klscrape` WHERE `Station ID` = %(x)s")
   concursor.execute(state, {'x': x })
   result = concursor.fetchall()
   print(result)
   df['warning'][i] = result
for i in range(24,33):
   x = df['statid'][i]
   state = ("SELECT Danger FROM `klscrape` WHERE `Station ID` = %(x)s")
   concursor.execute(state, {'x': x })
   result = concursor.fetchall()
   print(result)
   df['danger'][i] = result
for i in range(24,33):
   x = df['statid'][i]
   state = ("SELECT `Station Name` FROM `klscrape` WHERE `Station ID` = %(x)s")
   concursor.execute(state, {'x': x })
   result = concursor.fetchall()
   print(result)
   df['stationname'][i] = result
for i in range(24,33):
   x = df['statid'][i]
   state = ("SELECT `Last Update` FROM `klscrape` WHERE `Station ID` = %(x)s")
   concursor.execute(state, {'x': x })
   result = concursor.fetchall()
   print(result)
   df['lastupdate'][i] = result


df['value'] = df['value'].astype(str).str.replace(r'\(|\)|,|,', '')
df['alert'] = df['alert'].astype(str).str.replace(r'\(|\)|,|,', '')
df['warning'] = df['warning'].astype(str).str.replace(r'\(|\)|,|,', '')
df['danger'] = df['danger'].astype(str).str.replace(r'\(|\)|,|,', '')
df['stationname'] = df['stationname'].astype(str).str.replace(r'\(|\)|,|,', '')
df['stationname'] = df['stationname'].astype(str).str.replace("[']", "", regex=True)
df['lastupdate'] = df['lastupdate'].astype(str).str.replace(r'\(|\)|,|,', '')
df['lastupdate'] = df['lastupdate'].astype(str).str.replace("[']", "", regex=True)

df = df.astype({"value": float, "alert": float, "warning":float, "danger":float})


compare = []
level = []
for i in range(0,len(df)):
    if df['value'][i] > df['danger'][i]:
        compare.append("http://maps.google.com/mapfiles/ms/icons/red-dot.png")
        level.append("Danger Level")
    elif df['value'][i] > df['warning'][i]:
        compare.append("http://maps.google.com/mapfiles/ms/icons/orange-dot.png")
        level.append("Warning Level")
    elif df['value'][i] > df['alert'][i]:
        compare.append("http://maps.google.com/mapfiles/ms/icons/yellow-dot.png")
        level.append("Alert Level")
    elif df['value'][i] < df['alert'][i]:
        compare.append("http://maps.google.com/mapfiles/ms/icons/green-dot.png")
        level.append("Normal Level")
df['Danger Level'] = compare
df['Current Status'] = level

df.rename(columns = {'lat':'latitude','long':'longitude','stationname':'name','statid':'info','Danger Level':'icon'}, inplace = True)

engine = create_engine("mysql+pymysql://root:@localhost/myfloodlaravel")
df.to_sql('locations', con = engine, if_exists='replace', index=False)