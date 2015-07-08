import urllib as ul
import mysql.connector as sql
from ftplib import FTP

#############################################################################################################
###											CreateFromWeb Class											  ###
#############################################################################################################

# Create tables in MySQL database for all NYSE and NASDAQ stocks
class CreateFromWeb:

	# Init
	def __init__(self):	
		self.user = "root"
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

		# Create Company table
		self.createCompanyTable()

		# Populate NYSE Entries
		self.setupAndRunFTP("ftp.nasdaqtrader.com", "SymbolDirectory", "otherlisted.txt", self.addNYSE)
		print "Finished adding NYSE entries"

		# Populate NASDAQ Entries
		# self.setupAndRunFTP("ftp.nasdaqtrader.com", "SymbolDirectory", "nasdaqlisted.txt", self.addNASDAQ)
		# print "Finished adding NASDAQ entires"

		# Populates MySQL database with NYSE and NASDAQ data
		self.createAllCompanyDataTables()
		print "Finished creating company data tables"

		# Cleanup
		self.cleanupSQL()
		print "Done"

	#############################################################################################################
	###												Setup Functions											  ###
	#############################################################################################################

	# Setup SQL Connection
	def setupSQL(self):

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

	# Creates company table that lists all companies in NYSE NASDAQ
	def createCompanyTable(self):

		# Create and execute query to create company table if necessary
		if (self.tableDoesNotExist("company")):

			query = "CREATE TABLE company ( \
				ticker_id VARCHAR(16) NOT NULL, \
				exchange VARCHAR(64) NOT NULL, \
				sector VARCHAR(128) NOT NULL, \
				of_interest TINYINT(8) NOT NULL, \
				name VARCHAR(256) NOT NULL, \
				primary key (ticker_id) \
			);"

			self.executeSQL(query) 

			# Commit changes
			self.sqlConnection.commit()

			print "Created company table"


	# Add NYSE stock to company table in SQL database
	def addNYSE(self, companyStr):

		# Ignore first and last lines
		if companyStr[:10] != "ACT Symbol" and companyStr[:5] != "File ":
				
				companyArr = companyStr.split('|')
				
				# Check length and only add stocks from NYSE 
				if len(companyArr) == 8 and companyArr[2] == 'N':

					# Get, sanitize, and commit company data
					self.addCompanyToTable(self.sanitizeInput(companyArr[0]), self.sanitizeInput(companyArr[1]), "NYSE", "NONE", 0)
					
	# Add NASDAQ stock to company table in SQL database
	def addNASDAQ(self, companyStr):
		
		# Ignore first and last lines
		if companyStr[:7] != "Symbol|" and companyStr[:5] != "File ":

				companyArr = companyStr.split('|')
				
				# Check length 
				if len(companyArr) == 6:

					# Get, sanitize, and commit company data
					self.addCompanyToTable(self.sanitizeInput(companyArr[0]), self.sanitizeInput(companyArr[1]), "NASDAQ", "NONE", 0)
					

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

	def createAllCompanyDataTables(self):

		# Get all ticker IDs
		ticker_ids = self.executeSQL("SELECT ticker_id from company;")

		# Create company table for each ticker ID
		for ticker_id in ticker_ids:

			# Check if company table already exists
			# Create company table if necessary
			if (self.tableDoesNotExist(ticker_id[0] + "_data")):
				self.createCompanyDataTable(self.sanitizeInput(ticker_id[0]))
			else:
				print "Table already exists for company: %s" % ticker_id[0]

	# Create a company data table in the SQL database
	def createCompanyDataTable(self, tickerID):

		# Create and execute query to create a company data table
		query = "CREATE TABLE %s_data ( \
			time DATETIME NOT NULL, \
			open DOUBLE NOT NULL, \
			high DOUBLE NOT NULL, \
			low DOUBLE NOT NULL, \
			close DOUBLE NOT NULL, \
			volumn INTEGER(10) NOT NULL, \
			adj_close DOUBLE NOT NULL, \
			primary key (time) \
		);" % (tickerID)

		self.executeSQL(query) 

		# Commit changes
		self.sqlConnection.commit()

		print "Created table for company: %s" % tickerID 

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

	# Execute an SQL query and clear and return buffer with fetchall()
	def executeSQL(self, query):

		returnResults = None
		cursorResults = self.cursor.execute(query, multi=True)
		
		for result in cursorResults:
			if result.with_rows:
				returnResults = result.fetchall()

		return returnResults

	# Checks if SQL database has table with name tableName
	def tableExists(self, tableName):

		return not (len(self.executeSQL("SHOW TABLES LIKE '%s'" % (tableName))) == 0)

	# Checks if SQL database doesn't have table with name tableName
	def tableDoesNotExist(self, tableName):

		return (len(self.executeSQL("SHOW TABLES LIKE '%s'" % (tableName))) == 0)


# Run CreateFromWeb's main() for calls to main()
if __name__ == "__main__":
	
	createFromWeb = CreateFromWeb()
	createFromWeb.main()
