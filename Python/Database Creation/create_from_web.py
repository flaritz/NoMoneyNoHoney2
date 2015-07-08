import urllib as ul
import mysql.connector as sql
from ftplib import FTP

#############################################################################################################
###											CreateFromWeb Class											  ###
#############################################################################################################

# Creates tables in MySQL database for all Nasdaq and NYSE stocks
class CreateFromWeb:

	# Init
	def __init__(self):	
		self.user = "flaritz"
		self.password = "Skippy12**"
		self.host = "127.0.0.1"
		self.database = "no_money_no_honey_2"
		self.sqlConnection = None
		self.cursor = None

	#############################################################################################################
	###												     Main			  									  ###
	#############################################################################################################

	def main(self):

		# Setup SQL Connection
		self.setupSQL()

		# Populate NYSE Entries
		self.setupAndRunFTP("ftp.nasdaqtrader.com", "SymbolDirectory", "otherlisted.txt", self.addNYSE)
		print "Finished adding NYSE entries"

		# Populate Nasdaq Entries
		# self.setupAndRunFTP("ftp.nasdaqtrader.com", "SymbolDirectory", "nasdaqlisted.txt", self.addNASDAQ)
		# print "Finished adding NASDAQ entires"

		# Populates MySQL database with Nasdaq and NYSE data
		self.createCompanyStockTables()
		print "Finished creating individual company tables"

		# Cleanup
		self.cleanupSQL()
		print "Done"

	#############################################################################################################
	###												Setup Functions											  ###
	#############################################################################################################

	def setupSQL(self):

		# self.sqlConnection = sql.connect(user='mboch', password='Macallan2181', host='127.0.0.1', database='no_money_no_honey_2')
		self.sqlConnection = sql.connect(user=self.user, password=self.password, host=self.host, database=self.database)
		self.cursor = self.sqlConnection.cursor()
		print "SQL Connected: %s" % self.database

	# Setup and Run FTP Connection
	def setupAndRunFTP(self, urlName, directoryName, fileName, function):

		# Create FTP
		ftp = FTP(urlName)
		ftp.login()
		print "FTP Connected: %s" % urlName

		# Change directory
		ftp.cwd(directoryName)

		# Run FTP
		ftp.retrlines("RETR " + fileName, function)


	#############################################################################################################
	###												Table Functions											  ###
	#############################################################################################################

	# Add NYSE stock to company table in SQL database
	def addNYSE(self, companyStr):

		# Ignore first and last lines
		if companyStr[:10] != "ACT Symbol" and companyStr[:4] != "File":
				
				companyArr = companyStr.split('|')
				
				# Check length and only add stocks from NYSE 
				if len(companyArr) == 8 and companyArr[2] == 'N':

					# Get, sanitize, and commit company data
					self.addCompanyToTable(self.sanitizeInput(companyArr[0]), self.sanitizeInput(companyArr[1]), "NYSE", "NONE", 0)
					
	# Add Nasdaq stock to company table in SQL database
	def addNASDAQ(self, companyStr):
		None

	# Add a company to company table in SQL database
	def addCompanyToTable(self, tickerID, name, exchange, sector, interest):

		# Create and execute query to add company if it's not already present in table
		query = "INSERT INTO company (ticker_id, exchange, sector, of_interest, name) \
					SELECT * FROM (SELECT '%s' as ticker_id, '%s' as exchange, '%s' as sector, %d as of_interest, '%s' as name) AS tmp \
					WHERE NOT EXISTS ( \
						SELECT ticker_id FROM company HAVING ticker_id = '%s' \
					) LIMIT 1;" % (tickerID, exchange, sector, interest, name, tickerID)
		
		self.executeSQL(query)

		# Commit changes
		self.sqlConnection.commit()

	def createCompanyStockTables(self):
		None
		# query = "SELECT ticker_id from company;"
		# results = c.execute(query, multi=True)
		# for result in results:
		# 	if result.with_rows:
		# 		ticker_ids = result.fetchall()
		# 		for ticker_id in ticker_ids:
		# 			print ticker_id[0]
		# 			query = "SHOW TABLES LIKE '%s_data'" % (ticker_id[0])
		# 			results2 = c.execute(query, multi=True)
		# 			for result2 in results2:
		# 				if result2.with_rows:
		# 					tables = result2.fetchall()
		# 					if len(tables) == 0:
		# 						print 'not there'
		# 						query = "CREATE TABLE %s_data ( \
		# 									time DATETIME NOT NULL, \
		# 									open DOUBLE NOT NULL, \
		# 									high DOUBLE NOT NULL, \
		# 									low DOUBLE NOT NULL, \
		# 									close DOUBLE NOT NULL, \
		# 									volumn INTEGER(10) NOT NULL, \
		# 									adj_close DOUBLE NOT NULL, \
		# 									primary key (time) \
		# 								);" % (sanitizeInput(ticker_id[0]))
		# 						results3 = c.execute(query, multi=True)
		# 						for result3 in results3:
		# 							if result3.with_rows:
		# 								result3.fetchall()

	#############################################################################################################
	###												Data Functions											  ###
	#############################################################################################################

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

	#############################################################################################################
	###												Cleanup Functions										  ###
	#############################################################################################################

	def cleanupSQL(self):

		self.cursor.close()
		self.sqlConnection.close()

	#############################################################################################################
	###												Helper Functions										  ###
	#############################################################################################################

	# Remove unwanted characters from inputStr and returns a copy
	def sanitizeInput(self, inputStr):
	
		inputStr = inputStr.replace("'", '')
		inputStr = inputStr.replace(";", '')
		inputStr = inputStr.replace(".", '_')
		return inputStr

	# Execute an SQL query and clear buffer with fetchall()
	def executeSQL(self, query):

		cursorResults = self.cursor.execute(query, multi=True)
		
		for result in cursorResults:
			if result.with_rows:
				result.fetchall()


# Run CreateFromWeb's main() for calls to main()
if __name__ == "__main__":
	
	createFromWeb = CreateFromWeb()
	createFromWeb.main()