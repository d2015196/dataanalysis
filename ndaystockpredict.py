import sqlite3
import pandas as pd
import numpy as np
import booleanchops2
from sklearn import svm
from sklearn.externals import joblib
import pickle
from sklearn.metrics import accuracy_score

#predict whether the stock goes up or down within n days, n will be experimented with. 
#data will be 10-day, 20-day, 30-day, 100-day and 200-day average. 
#so if n is 8, i will be asking from today what will the price be 8 days from now (current + n)

a = input("Select database: ")
n = input("Select time frame for prediction: ")

conn = sqlite3.connect(a)
conn.row_factory = lambda cursor, row: row[0]
c = conn.cursor()


#determine end date based on the time frame,  "n"

date_info = a.split("v")
end_datefile = date_info[2]
c.execute("SELECT Date FROM Stocks ORDER BY Date DESC LIMIT " + str(n) + ", 1")
end_date = c.fetchone()


#determine the earliest date that all the tables share. 

training_data = input("Select MA to train model: ") #in the form of 10, 30, 100 ... 

list = training_data.split(", ")

#if the list contains only one element then the training data set starting date is the same as that of the table. Let's access this 
#first element 

if(len(list) == 1): 
	c.execute("SELECT Date FROM dayavg" + list[0] +" ORDER BY Date ASC LIMIT 1")
	starting_date = c.fetchone()
 
#otherwise we need to look at the first date value shared by all the tables indicated by the user. We will do this 
#by executing the "statement" on sqlite

else: 

	statement = ""
	i = 0
	while(i < len(list) - 1): 
		statement = statement + "SELECT Date FROM dayavg" + list[i]+ " INTERSECT "
		i = i + 1
	else: 
		statement = statement + "SELECT Date FROM dayavg" + list[i] + " ORDER BY Date ASC LIMIT 1"

	c.execute(statement)

	starting_date = c.fetchone()

#Now that we have the starting date we will collect the data and store it in numpy arrays 

#list that will store the lists returned by the various fetch all statements 
data_list = []

#am i not using sqlite columns to the fullest advantage ? 
c.execute("SELECT Date FROM Stocks WHERE Date >= '" + starting_date + "' AND Date <= '"+ end_date + "' ORDER BY Date ASC")
dates = c.fetchall()
name = booleanchops2.boolean_table(conn, n, dates)


c.execute("SELECT AdjClose FROM Stocks WHERE Date >= '" + starting_date + "' AND Date <= '"+ end_date + "' ORDER BY Date ASC")
data_list.append(c.fetchall())


for i in list: 
	c.execute("SELECT AdjCloseavg FROM dayavg" + str(i) + " WHERE Date >= '" + starting_date + "' AND Date <= '"+ end_date +"' ORDER BY Date ASC") 
	data_list.append(c.fetchall())


#target list 
c.execute("SELECT Boolean FROM " + str(name) +" ORDER BY Date ASC")
target_list = c.fetchall()

#now I actually turn the lists into numpy arrays 
numpy_data = np.array(data_list)
numpy_data = numpy_data.transpose()
numpy_target = np.array(target_list)
numpy_target = numpy_target.transpose()


#split data and target into for-training (70 % ) and for-testing sets (30 %)

entries_in_numpy = numpy_data.shape[0]

split = round(entries_in_numpy * 0.7)

#train 

clf = svm.SVC() #houston's clf
clf.fit(numpy_data[:split,:], numpy_target[:split])

s = pickle.dumps(clf)


clf2 = pickle.loads(s)

predicted = clf2.predict(numpy_data[split:entries_in_numpy, :])

results = numpy_target[split:entries_in_numpy]

accuracy = accuracy_score(results, predicted, normalize = True, sample_weight= None)

print("accuracy =  " + str(accuracy * 100) + "%")
