#this program creates a table in a user-specified database (.db) that contains a x day average column, where x is a user-defined integer. 
import sqlite3

def dayaveragetables(conn, c, database, day):

	newname = "dayavg" + str(day) 
	#create the table 
	c.execute("CREATE TABLE " + newname + " (Date text, AdjClose real, AdjCloseavg real)")

	#this function inserts the values into the xdayavg table. 
	#def insertSQL (day, offset, con, d): 
	def insertSQL (day, offset):
		#c.execute("INSERT INTO a SELECT Date, AdjClose, avg(AdjClose) FROM (SELECT * FROM Stocks ORDER BY Date ASC LIMIT " + str(offset) + ", " + str(day) + ")")
		c.execute("INSERT INTO " + newname + " SELECT Date, AdjClose, avg(AdjClose) FROM (SELECT * FROM Stocks ORDER BY Date ASC LIMIT " + str(offset) + ", " + str(day) + ")")

	#get the number of entries in the original table, Stocks 
	c.execute("SELECT count(Date) FROM Stocks")
	totalentry = c.fetchone()[0]

	#insert the values using the insertSQL() function
	#c.execute("BEGIN TRANSACTION")

	i = 0

	while totalentry - i - int(day) >= 0: 
		#insertSQL(day, i, con, d)
		insertSQL(day, i)
		i = i + 1

	#rename table
	#newname = "dayavg" + str(day) 
	#d.execute("ALTER TABLE a RENAME TO " + newname)

	#commit changes. 
	#d.commit()

