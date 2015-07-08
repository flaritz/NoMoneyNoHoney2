import urllib as ul
import mysql.connector as sql

from ftplib import FTP

conn = sql.connect(user='mboch', password='Macallan2181', host='127.0.0.1', database='no_money_no_honey_2')
c = conn.cursor()

def sanitizeInput(inputStr):
	inputStr = inputStr.replace("'", '')
	inputStr = inputStr.replace(";", '')
	inputStr = inputStr.replace(".", '_')
	return inputStr

def addToCompanyTable(companyStr):
	if companyStr[:10] != "ACT Symbol" and companyStr[:4] != "File":
			companyArr = companyStr.split('|')
			if len(companyArr) == 8 and companyArr[2] == 'N':
				companyTickerID = sanitizeInput(companyArr[0])
				companyName = sanitizeInput(companyArr[1])
				companyExchange = 'NYSE'
				companySector = 'NONE'
				companyInterest = 0
				
				query = "INSERT INTO company (ticker_id, exchange, sector, of_interest, name) \
							SELECT * FROM (SELECT '%s' as ticker_id, '%s' as exchange, '%s' as sector, %d as of_interest, '%s' as name) AS tmp \
							WHERE NOT EXISTS ( \
	    						SELECT ticker_id FROM company HAVING ticker_id = '%s' \
							) LIMIT 1;" % (companyTickerID, companyExchange, companySector, companyInterest, companyName, companyTickerID)
				results = c.execute(query, multi=True)
				for result in results:
					if result.with_rows:
						result.fetchall()
				conn.commit()

def createCompanyStocktables():
	query = "SELECT ticker_id from company;"
	results = c.execute(query, multi=True)
	for result in results:
		if result.with_rows:
			ticker_ids = result.fetchall()
			for ticker_id in ticker_ids:
				print ticker_id[0]
				query = "SHOW TABLES LIKE '%s_data'" % (ticker_id[0])
				results2 = c.execute(query, multi=True)
				for result2 in results2:
					if result2.with_rows:
						tables = result2.fetchall()
						if len(tables) == 0:
							print 'not there'
							query = "CREATE TABLE %s_data ( \
										time DATETIME NOT NULL, \
										open DOUBLE NOT NULL, \
										high DOUBLE NOT NULL, \
										low DOUBLE NOT NULL, \
										close DOUBLE NOT NULL, \
										volumn INTEGER(10) NOT NULL, \
										adj_close DOUBLE NOT NULL, \
										primary key (time) \
									);" % (sanitizeInput(ticker_id[0]))
							results3 = c.execute(query, multi=True)
							for result3 in results3:
								if result3.with_rows:
									result3.fetchall()

#g=d -- daily
# ul.urlretrieve("http://real-chart.finance.yahoo.com/table.csv?s=GOOG&a=02&b=27&c=2014&d=06&e=7&f=2015&g=d&ignore=.csv", "F:/Libraries/Desktop/butts.csv")

# params = ul.urlencode({'s': 'GOOG', 'a': 2, 'b': 27, 'c': 2014, 'd': 6, 'e': 7, 'f':2015, 'g':'d', 'ignore':'.csv'})
# f = ul.urlopen("http://real-chart.finance.yahoo.com/table.csv?%s" % params)
# # print f.read()
# csvstr = f.read()
# csvarr = csvstr.split("\n")

# reader = csv.reader(csvarr)
# print reader
# for row in reader:
# 	print row

# for row in csvarr[1:-1]:
# 	rowarr = row.split(',')
# 	for column in rowarr:
# 		print column
# 		# make column

ftp = FTP("ftp.nasdaqtrader.com")
ftp.login()
print 'ftp connected'
ftp.cwd('SymbolDirectory')
ftp.retrlines('RETR otherlisted.txt', addToCompanyTable)

print 'company table created'

createCompanyStocktables()

c.close()
conn.close()