
import pymysql
db=pymysql.connect(host='localhost',port=3306,user='root',passwd='',db='mysql',charset='utf8')
cursor=db.cursor()
sql='SELECT address from companyrank'
cursor.execute(sql)
a=cursor.fetchall()
#print(a)
for i in a:
      i=str(i).replace('(','').replace(')','').replace(',','')
      i=i.replace("'",'').split('市')[0]
      print(i)
      try:
      	b=i.split('省')[0]
      	d=i.split('省')[1]
      except:
      	b,d=i,i

      print(b,d)
