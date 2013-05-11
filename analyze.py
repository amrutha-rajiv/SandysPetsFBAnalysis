#python libraries
import sys, time
from datetime import datetime
#third party libraries
import sqlite3 as sql
import requests, simplejson, nltk

IRRELEVANT_WORDS = ["and", "the", "for", "you", "this", "https", "with", "are", "that", "was", "she", "can", "not", "your", "his","has", "...", "they", "his", "has", "them", "one", "all", "but", "out", "i'm", "via", "http","its","it's"]

def getDBConnection(DB_NAME):
	try:
		con = sql.connect(DB_NAME)
		cur = con.cursor()	
		return cur,con
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)

def analyzeMessages(type="POST"):
	(cur,con) = getDBConnection('sandyspets6-8')
	if type="POST":
		results = cur.execute("select post from post_info;")
		file_name = 'post-freqdist.txt'

	elif type="COMMENT":
		results = cur.execute("select comment from user_comments;")
		file_name = 'comment-freqdist.txt'
	else:
		print 'invalid type'
		sys.exit()
	#compile the tuple of posts as a list of posts in python
	list_posts = [(list(result)) for result in results]
	post_frequencies = getFrequency(list_posts,file_name)

def getFrequency(list,file_name):
	global IRRELEVANT_WORDS
	fd = nltk.FreqDist()
	for element in list:
		element= element[0]
		if (element.encode('utf8')).strip() == "":
			continue
		for sentence in nltk.sent_tokenize(element.lower()):
			for word in nltk.word_tokenize(sentence):
				if len(word) <=2:
					continue
				if word in IRRELEVANT_WORDS:
					continue
				fd.inc(word)
	out_file = open(file_name,'w')
	for element in fd.keys():
		if fd[element] > 1:
			print (element.encode('utf8')+" "+str(fd[element]))			
			out_file.write(element.encode('utf8')+" "+str(fd[element])+"\n")



def analyze ():
	(cur,con) = getDBConnection('sandyspets')
	try:
		'''user-wise'''
		#number: mean min max
		#number of likes by each user
		results = cur.execute("select userid,username,count(*) as 'count_of_likes' from user_likes group by userid;")
		list_num_likes = [(list(result)) for result in results]
		#number of comments by each user
		results = cur.execute("select userid,username,count(*) as 'count_of_comments' from user_comments group by userid;")
		list_num_comments = [(list(result)) for result in results]
		#number of comments per post by each user
		results = cur.execute("select userid,username,post_id,comment,count(*) as 'count_of_comments' from user_comments group by userid, post_id;")
		list_num_commentsperpost = [(list(result)) for result in results]
		'''user-relative to time'''
		#time between user's comments: mean min max
		results = cur.execute("select distinct userid from user_comments order by userid,created_time;")
		list_users = [list(result)[0] for result in results]
		timestats_peruser = {}
		for user in list_users:
			query = "select userid, created_time from user_comments where userid="+user+" order by created_time,userid;"
			results = cur.execute(query)
			list_results = [list(result) for result in results]
			timestats_peruser[list_results[0][0]] = [datetime.strptime(result[1],'%Y-%m-%d %H:%M:%S') for result in list_results]
		#print timestats_peruser
		#numbr of words in a comment: mean, min, max
		comments_peruser = {}
		for user in list_users:
			query = "select comment from user_comments where userid="+user+";"
			results = cur.execute(query)
			comments_peruser[user] = [result[0] for result in results]	
		#print comments_peruser		
		comments_peruser_mean ={}
		sum_comments_length = 0
		total_comments = 0
		for user in comments_peruser.keys():
			comment_length = [len(comment) for comment in comments_peruser[user]]
			total_length = sum(comment_length)
			mean = float(total_length)/float(len(comments_peruser[user]))
			print 'mean: ',str(mean)
			comments_peruser_mean[user] = mean
			sum_comments_length += total_length
			total_comments += len(comments_peruser[user])

		#numbr of words in a comment for all users
		mean_commentlength  = float(sum_comments_length)/float(total_comments)
		print 'mean_commentlength: '+str(mean_commentlength)

	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)   
	finally: 
	    if con:
	        con.close()

	
		'''day-wise'''
		#number of posts per day
		#select * from post_info group by strftime("%Y-%m-%d",created_time);
		#number of comments per day
		#select * from user_comments group by strftime("%Y-%m-%d",created_time);

		'''relative to time'''
		#time between posts on the page: mean, min, max
		#select * from post_info order by created_time;
		#select * from post_info order by updated_time;
		#time between comments: mean min max
		#select * from user_comments order by created_time;
		#time between posts/comments: mean min max

analyzeMessages(type="COMMENT")