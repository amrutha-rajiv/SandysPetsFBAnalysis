#python libraries
import re, random,sys,array,math
from datetime import datetime
import sqlite3 as sql
from time import  strptime,mktime
#local
from analyze import getDBConnection
import os
#3rd party libraries
import nltk
from nltk.classify import util,NaiveBayesClassifier, DecisionTreeClassifier

def mean_list(list_values):
	if len(list_values) > 0:
		return sum(list_values)/len(list_values)
	else:
		return 0

def stddev_list(list_values):
	mean_val = mean_list(list_values)
	if len(list_values) > 0:
		return math.sqrt(sum((mean_val-val)**2 for val in list_values))/len(list_values)
	else:
		return 0

def createTables(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
	cur.execute('CREATE TABLE IF NOT EXISTS logs(activityid INT,userid TEXT, activity TEXT, objectid TEXT, object_type TEXT, activity_timestamp TIMESTAMP);')
	cur.execute('CREATE TABLE IF NOT EXISTS logs_petmatch(activityid INT,userid TEXT, activity TEXT, objectid TEXT, objectid2 TEXT, activity_timestamp TIMESTAMP);')

def convertToTimestamp(date,type="apache"):
	#timeval=strptime(timestamp,'%d/%m/%Y:%H:%M:%S')

	if type=="apache":
		return datetime.fromtimestamp(mktime(strptime(date,'%d/%b/%Y:%H:%M:%S')))
	elif type=="activity-log":
		#Mon Feb 18:46:05 2013
		return datetime.fromtimestamp(mktime(strptime(daete,'%a %b %d %H:%M:%S %Y')))

def apacheFileToDB(log_file_name,DB_NAME):
	ACTIVITY_STRING = {"PETMATCH:GET":"PETMATCH_VIEW","PETREPORT:GET":"PETREPORT_VIEW",}
	#make this work for other requests as get petmatch
	log_file = open(log_file_name,'r')
	log_expr = re.compile("\d+\.\d+\.\d+\.\d+ - - \[([^\[\]:]+:\d+:\d+:\d+) \+(\d{4})\] (\".+\") (\d{3}) (.+) (\d{1,10})")
	(cur,con) = getDBConnection(DB_NAME)
	PETMATCH_ACTIVITY_ID = cur.execute("SELECT count(*) from logs_petmatch;").next()[0]
	ACTIVITY_ID = cur.execute("SELECT count(*) from logs;").next()[0]

	for line in log_file:
		pattern_match = log_expr.match(line)
		if not pattern_match is None:
			date = pattern_match.group(1)
			time = pattern_match.group(2)
			request_string = pattern_match.group(3)
			status_code = pattern_match.group(4)#check status code before inserting
			username = pattern_match.group(5)
			userid = pattern_match.group(6)	
			request_string = (re.split("\"",request_string)[1]).split()
			method = request_string[0]
			request = re.split("/",request_string[1])
			object_type = request[2].upper()
			object_id= request[3]
			if object_type == "PROPOSE_PETMATCH":
				activity = object_type
				objectid2 = request[4]
				PETMATCH_ACTIVITY_ID += 1
				cur.execute('INSERT INTO logs_petmatch values(?,?,?,?,?,?)',[PETMATCH_ACTIVITY_ID,username,activity,object_id,objectid2,convertToTimestamp(date)])
			else:
				ACTIVITY_ID += 1
				cur.execute('INSERT INTO logs values(?,?,?,?,?,?)',[ACTIVITY_ID,username,ACTIVITY_STRING[object_type+":"+method],object_id,object_type,convertToTimestamp(date)])	
				
			#print date+" "+time+" "+request_string+" "+status_code+" "+username+" "+userid		
	
		else:
			print str(pattern_match)
	con.commit()

def activityFileToDB(log_file_name,DB_NAME):

	log_expr = re.compile("\[(\w{3} \w{3} \d{2} \d{2}:\d{2}:\d{2} \d{4})\] \[(\w+)\] (\w+) .+ ID\{(\d+)\}")
	ACTIVITY_STRING = {""}
	log_file = open(log_file_name,'r')
	(cur,con) = getDBConnection(DB_NAME)
	ACTIVITY_ID = cur.execute("select count(*) from logs;").next()[0]
	for line in log_file:
		pattern_match = log_expr.match(line)
		if not pattern_match is None:
			date = pattern_match.group(1)
			username = pattern_match.group(3)
			#userid = pattern_match.group(6)	
			activity = pattern_match.group(2)
			object_id= pattern_match.group(4)
			object_type = re.split('_',activity)[0]
			#print date+" "+time+" "+request_string+" "+status_code+" "+username+" "+userid
			ACTIVITY_ID += 1
			cur.execute('INSERT INTO logs values(?,?,?,?,?,?)',[ACTIVITY_ID,username,activity,object_id,object_type,convertToTimestamp(date,type="activity-log")])
			
		else:
			print str(pattern_match)
	con.commit()

#NLTK FEATURE FORMAT: [{user-feature-set},'class']
def constructFeatures(DB_NAME,userid):
	#will have to create # of columns in each user feature list = num of features
	(cur,con) = getDBConnection(DB_NAME)
	user_features={}
	#create skeleton for all features

	#Feature 1 & 2: Mean, Stddev of time between any two activities by a user
	#results = cur.execute("SELECT userid from logs2 group by userid having count(userid)>1;")	
	#list_users = [list(result)[0].encode('ascii','ignore') for result in results]
	#for user in list_users:
	query = "SELECT activity_timestamp from logs where userid=\""+userid.strip()+"\" order by activity_timestamp;"
	print query
	results = cur.execute(query)
	list_results = [result[0] for result in results]
	timestats=[]
	if len(list_results)>1:
		for i in range(len(list_results)):
			if i==1:
				continue
			time_diff = datetime.strptime(list_results[i],'%Y-%m-%d %H:%M:%S')-datetime.strptime(list_results[i-1],'%Y-%m-%d %H:%M:%S')
			timestats.append(abs(time_diff.total_seconds()))	
		#print str(timestats)
		user_mean = mean_list(timestats)
		user_stddev = stddev_list(timestats)
		print "mean: %s stddev: %s" %(str(user_mean),str(user_stddev))
		
	else:
		user_mean = 0
		user_stddev= 0
	user_features["f1"]	= user_mean
	user_features["f2"] = user_stddev
	
	print "Feature 1&2 complete"	
	
	#Feature 3,4,5: Mean, Stddev of time between any two pet matches proposed, Feature 3: number of pet matches proposed
	#for user in user_features.keys():
	results = cur.execute("SELECT activity_timestamp from logs where userid=\""+userid.strip()+"\" and activity=\"PETMATCH_PROPOSED\" order by activity_timestamp;")
	list_results = [result[0] for result in results]
	timestats=[]
	if len(list_results)>1:
		for i in range(len(list_results)):
			if i==1:
				continue
			time_diff = datetime.strptime(list_results[i],'%Y-%m-%d %H:%M:%S')-datetime.strptime(list_results[i-1],'%Y-%m-%d %H:%M:%S')
			timestats.append(abs(time_diff.total_seconds()))	
		#print str(timestats)
		user_mean = mean_list(timestats)
		user_stddev = stddev_list(timestats)
		print "mean: %s stddev: %s" %(str(user_mean),str(user_stddev))
		
	else:
		user_mean = 0
		user_stddev= 0
	user_features["f3"] = user_mean
	user_features["f4"]	= user_stddev 
	user_features["f5"] = len(list_results)
	print "Feature 3,4&5 complete"

	#Feature 6&7: mean $ stddev number of bookmarks per day
	#for user in user_features.keys():
	results = cur.execute("SELECT strftime('%d/%m/%Y',activity_timestamp) as \"time\", count(\"time\") from logs where activity=\"PETREPORT_ADD_BOOKMARK\"AND USERID=\""+userid.strip()+"\"  group by time;")
	list_results = [result[1] for result in results]
	numstats=[]
	if len(list_results)>1:
		for i in range(len(list_results)):
			if i==1:
				continue
			numstats.append(list_results[i])	
		#print str(timestats)
		user_mean = mean_list(numstats)
		user_stddev = stddev_list(numstats)
		print "mean: %s stddev: %s" %(str(user_mean),str(user_stddev))
		
	else:
		user_mean = 0
		user_stddev= 0
		user_features["f6"] = user_mean,
		user_features["f7"] = user_stddev
		
	print "Feature 6&7 complete"
	

	#Feature x: average time between viewing and proposing a petmatch for the same pet		
	#select a.userid, a.activity_timestamp as "viewed_time",b.activity_timestamp as "matched_time" from logs a,logs_petmatch b where (a.objectid=b.objectid or a.objectid=b.objectid2) and a.activity="PETREPORT_VIEW" AND a.userid=b.userid;
	#INSUFFICIENT DATA: only 1 instance each of 7 users available. 

	#Feature 8: number of bookmarks
	results = cur.execute("SELECT userid, count(userid) from logs where activity=\"PETREPORT_ADD_BOOKMARK\" AND USERID=\""+userid.strip()+"\" group by userid;")
	list_results = [[result[0],result[1]] for result in results]	
	if len(list_results) >0:
		[user,count_bookmarks] = list_results[0]
	else:
		count_bookmarks = 0
	user_features["f8"] = count_bookmarks	
	print "Feature 8 complete"

	#Feature 9: number of votes
	results = cur.execute("SELECT userid, count(userid) from logs where (activity=\"PETMATCH_UPVOTE\" or activity=\"PETMATCH_UPVOTE\") AND USERID=\""+userid.strip()+"\"  group by userid;")
	list_results = [[result[0],result[1]] for result in results]	
	if len(list_results) >0:
		[user,count_votes] = list_results[0]
	else:
		count_votes = 0
	user_features["f9"] = count_votes
	print "Feature 9 complete"

	#Feature 10: number of pet match views
	results = cur.execute("SELECT userid, count(userid) from logs where activity=\"PETMATCH_VIEW\" AND USERID=\""+userid.strip()+"\"  group by userid;")
	list_results = [[result[0],result[1]] for result in results]	
	if len(list_results) >0:
		[user,count_views] = list_results[0]
	else:
		count_views=0
	user_features["f10"] = count_views
	print "Feature 10 complete"

	#Feature 11&12, 13&14 : Ratio of number of bookmarks OR VIEWS: Number of pet reports per day
	#GROUP COUNT OF PETREPORTS BY DAY-MONTH AND STORE
	results = cur.execute("SELECT strftime(\"%m-%d\",created_time) as date, count(petreport_id) from PETREPORT_MAPPING where created_time!=\"\" group by date ")
	list_date_counts = dict([[result[0].encode('ascii','ignore'),result[1]] for result in results])
	#FOR EACH USER, GROUP COUNT OF BOOKMARKS BY DAY-MONTH. 
	results = cur.execute("SELECT strftime(\"%m-%d\",activity_timestamp) as date, count(objectid) from logs where activity=\"PETREPORT_ADD_BOOKMARK\" AND USERID=\""+userid.strip()+"\" group by userid,date;")
	list_user_bookmarks = [[result[0].encode('ascii','ignore'),result[1]] for result in results]
	list_count_bookmarks = 	[]
	for [date,count_bookmarks] in list_user_bookmarks:
		if date in list_date_counts:
			ratio = count_bookmarks/list_date_counts[date]
		else:
			ratio = count_bookmarks

		list_count_bookmarks.append(ratio)

	mean_bookmark_count = mean_list(list_count_bookmarks)	
	stddev_bookmark_count = stddev_list(list_count_bookmarks)
	#FOR EACH USER, GROUP COUNT OF VIEWS BY DAY-MONTH
	results = cur.execute("SELECT strftime(\"%m-%d\",activity_timestamp) as date, count(objectid) from logs where activity=\"PETREPORT_VIEW\" AND USERID=\""+userid.strip()+"\" group by userid,date;")
	list_user_petviews = [[result[0].encode('ascii','ignore'),result[1]] for result in results]
	list_count_petviews = []
	for [date,count_petviews] in list_user_petviews:
		if date in list_date_counts:
			ratio = count_petviews/list_date_counts[date]
		else:
			ratio = count_petviews

		list_count_petviews.append(ratio)

	mean_petviews_count = mean_list(list_count_petviews)	
	stddev_petviews_count = stddev_list(list_count_petviews)	

	user_features["f11"] = mean_bookmark_count
	user_features["f12"] = stddev_bookmark_count
	user_features["f13"] = mean_petviews_count
	user_features["f14"] = stddev_petviews_count

	#CALCULATE BOOKMARK-RATIO, CALUCULATE PETREPORT VIEW RATIO. 
	print "Features 11,12,13,14 complete"
	
	#Feature 15: Number of submitted pet reports
	results = cur.execute("SELECT count(userid) from logs where activity=\"PETREPORT_SUBMITTED\" AND USERID=\""+userid.strip()+"\" group by userid;")
	list_results = [result[0] for result in results]	
	if len(list_results)>0:
		count_petreports = list_results[0]
	else:
		count_petreports = 0
	user_features["f15"] = count_petreports
	print "Feature 15 complete"

	return user_features

#NLTK FEATURE FORMAT: [{user-feature-set},'class']
def classify(DB_NAME):
	feature_file629 = open('feature_file629.txt','w')
	#get the userids for all the training data
	(cur,con) = getDBConnection(DB_NAME)
	results = cur.execute("SELECT user_id from user_mapping a, (SELECT userid,sum(count) from (select userid, count(comment) as count from user_comments where post_id not in (select * from duplicate_posts) group by userid union select author_id as userid, count(postid) as count from post_info where postid not in (select * from duplicate_posts) group by author_id) where userid!=\"403922739676650\" group by userid order by sum(count) desc limit 89) b where a.fbuser_id=b.userid;")
	list_users = ["user"+str(result[0]) for result in results]
	feature_vector = []
	class1_features = []
	class2_features = []
	for i in range(len(list_users)):
		if (i<28): 
			highly_active = True
			user_features = [constructFeatures (DB_NAME,list_users[i]),True]
			class1_features.append(user_features)
			feature_file629.write(str(user_features)+'\n')
		else:
			highly_active = False
			user_features =[constructFeatures (DB_NAME,list_users[i]),False]
			class2_features.append(user_features)
			feature_file629.write(str(user_features)+'\n')

	class1_cutoff = 22 #80% 22 70%: 20 80.77 acc with 70%
	class2_cutoff = 49 #80% 49 70%: 43 
	trainfeats = class1_features[:class1_cutoff]+class2_features[:class2_cutoff]
	testfeats = class1_features[class1_cutoff:]+class2_features[class2_cutoff:]
	classifier = NaiveBayesClassifier.train(trainfeats)
	#classifier = DecisionTreeClassifier.train(trainfeats)
	accuracy = nltk.classify.util.accuracy(classifier, testfeats)
	test_userid = "user24147"
	print "class of test_userid %s is :%s " %(test_userid,str(classifier.classify(constructFeatures(DB_NAME,test_userid))))
	test_userid2 = "user7973"
	print "class of test_userid2 %s is : %s" %(test_userid2,str(classifier.classify(constructFeatures(DB_NAME,test_userid2))))
	test_userid3="user18095"
	test_userid4="user28724"
	test_userid5="user6173"
	test_userid6="user11398"
	test_userid7="user13954"
	print "class of test_userid2 %s is : %s" %(test_userid3,str(classifier.classify(constructFeatures(DB_NAME,test_userid3))))
	print "class of test_userid2 %s is : %s" %(test_userid4,str(classifier.classify(constructFeatures(DB_NAME,test_userid4))))
	print "class of test_userid2 %s is : %s" %(test_userid5,str(classifier.classify(constructFeatures(DB_NAME,test_userid5))))
	print "class of test_userid2 %s is : %s" %(test_userid6,str(classifier.classify(constructFeatures(DB_NAME,test_userid6))))
	print "class of test_userid2 %s is : %s" %(test_userid7,str(classifier.classify(constructFeatures(DB_NAME,test_userid7))))


	# true_pos = 0
	# true_neg = 0
	# false_pos = 0
	# false_neg = 0
	# for (features,label) in testfeats:
	# 	if classifier.classify(features) == label:
	# 		if label == True:
	# 			true_pos += 1
	# 		else:
	# 			true_neg += 1
	# 	else:
	# 		if classifier.classify(features) == True:
	# 			false_pos += 1
	# 		else:
	# 			false_neg += 1
	# print "true pos: ", str(true_pos)
	# print "true_neg: ", str(true_neg)
	# print "false_pos: ", str(false_pos)
	# print "false_neg: ", str(false_neg)
	# print "accuracy is "+str(accuracy)
DB_NAME = 'sandyspets530628.db'
createTables(DB_NAME)
classify(DB_NAME)
#UNcomment below 4 lines to convert  activities from file to DB
# apacheFileToDB('simulated-apachelogs/apache_access.log',DB_NAME)
# FOLDER_NAME = 'simulated-activity-logs/'
# for fn in os.listdir(FOLDER_NAME):
#    	activityFileToDB(FOLDER_NAME+str(fn),DB_NAME)

#constructFeatures(DB_NAME)