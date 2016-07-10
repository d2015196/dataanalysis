import sqlite3
#takes in a list of dates and calculates their boolean values (did the price go up or down n days into the future from this current day?)
#the difference from booleanchops.py and this file is that this file takes into account the case
#where there is already an existing table "name" but perhaps the date entries are different than the ones 
#the user wants..... 


def boolean_table(conn, n, dates): 

	c = conn.cursor()
	name = "change" + str(n)
	
	#check if table's already there 
	c.execute("PRAGMA table_info (" + name + ")")

	message = c.fetchall()
	#if table doesn't exist... 
	if not message:
		c.execute("CREATE TABLE " + name + " (Date text, Boolean integer)")

	c.execute("BEGIN TRANSACTION")
	query = "INSERT INTO " + name + " VALUES (?, ?)"

	for date in dates: 
		c.execute("SELECT Date FROM " + name + " WHERE Date = '" + date+ "'" )

		#if entry is not in the table we need to fetch it and add it 
		if(c.fetchone() == None): 
			c.execute("SELECT AdjClose FROM Stocks WHERE Date = '" + date +"'")
			price1 = c.fetchone()
			subquery_price2 = "SELECT * FROM Stocks WHERE Date >= '" + date +"'"
			c.execute("SELECT AdjClose FROM (" + subquery_price2  +") ORDER BY Date ASC LIMIT " + str(n) + ", 1")
			price2 = c.fetchone()

			if price2 > price1: 
				upordown = 1

			else: 
				upordown = 0  
				 #if it didn't increase it is 0.

			c.execute(query, (date, upordown))

	
	#we trim the table to the span of dates we want... 
	start_date = dates[0]
	end_date = dates[len(dates) - 1]

	c.execute("DELETE FROM " + name + " WHERE Date > '" + end_date + "'")
	c.execute("DELETE FROM " + name + " WHERE Date < '" + start_date + "'")

	conn.commit()
	
	return(name)
		

