import sqlite3

#takes in a list of dates and calculates their boolean values (did the price go up or down n days into the future from this current day?)

def boolean_table(conn, n, dates): 

	c = conn.cursor()
	name = "change" + str(n)
	#check if table's already there 

	c.execute("PRAGMA table_info (" + name + ")")

	message = c.fetchall()

	if not message:
		c.execute("CREATE TABLE " + name + " (Date text, Boolean intger)")
		c.execute("BEGIN TRANSACTION")
		query = "INSERT INTO " + name + " VALUES (?, ?)"

		for date in dates: 
			c.execute("SELECT AdjClose FROM Stocks WHERE Date = '" + date +"'")
			price1 = c.fetchone()
			subquery_price2 = "SELECT * FROM Stocks WHERE Date >= '" + date +"'"
			c.execute("SELECT AdjClose FROM (" + subquery_price2  +") ORDER BY Date ASC LIMIT " + str(n) + ", 1")
			price2 = c.fetchone()

			if price2 > price1: 
				upordown = 1

			else: 
				upordown = 0  

			c.execute(query, (date, upordown))
		
		conn.commit()

	return(name)
		 #if it didn't increase it is 0.

