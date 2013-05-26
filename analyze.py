#python libraries
import sys, time,re
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

def getAlbumNameforPosts():
	#cannot match post with photo using ID, a message or smtg is reqd
	(cur,con) = getDBConnection('sandyspets6-8')	
	try:
		results = cur.execute('select album_name, photo_id from photos_albums;')
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)
	photo_ids ={}
	for row in results:
		photo_ids[row[1]]=row[0]
	out_file = open('status-compare.txt','w')
	list_of_files=['sandyspets-postid-lost.csv','sandyspets-postid-found.csv','sandyspets-postid-unknown.csv']
	file_status={'sandyspets-postid-lost.csv':'LOST','sandyspets-postid-found.csv':'FOUND' ,'sandyspets-postid-unknown.csv':'UKNOWN'}
	album_status={"Needs Foster or Adoption":"FOUND" , "NJ - FOUND CATS":"FOUND", "Their Journeys":"Journeys",\
	"NJ - LOST CATS":"LOST","NY - FOUND DOGS":"FOUND","NY - LOST DOGS":"LOST", "NY - FOUND CATS":"FOUND", \
	"NY - LOST CATS" :"LOST","NJ - LOST DOGS":"LOST", "NJ - FOUND DOGS":"FOUND",\
	"BrooklynACC Emergency Center":"UKNOWN",\
	"SAFE":"UNKNOWN","Missing Pieces":"UNKNOWN","Profile Pictures":"SKIP",\
	"Rainbow Bridge - RIP":"RIP", "In Temporary Foster":"FOUND","Adopted":"FOUND","NY and NJ BIRDS":"UNKNOWN",\
	"HELPFUL TIPS":"SKIP","Mitchell Field Emergency Shelter":"FOUND","Cover Photos":"SKIP"}
	for file_name in list_of_files:
		
		with open(file_name,'r') as f:
			list_ids = fileToList(f)
			for post_id in list_ids:
				#print 'iterating over file: '+file_name
				if post_id in photo_ids.keys():
					print_stmt= "%s\talbum:%s\tcategory:%s\n" %(post_id,photo_ids[post_id],file_status[file_name])
					out_file.write(print_stmt)
					print print_stmt	

def generatePetReports(posts_file="sandyspets-postid-lost.csv",pet_status="LOST"):
	(cur,con) = getDBConnection('sandyspets6-8')
	lost_csvfile = open(posts_file,'r')
	petreport_num = cur.execute('select count(*) from petreport_mapping').next()[0]
	list_postid = []
	for line in lost_csvfile:
		if 'post' in line:
			continue
		postid = re.split('\n',line)[0]
		list_postid.append(postid)
		petreport_num += 1
		cur.execute('insert into petreport_mapping values (?,?,?)',[postid,petreport_num,pet_status])
	con.commit()

def list_overlap(list_a,list_b):
	return any(i for i in list_a if i in list_b)

def fileToList(file_ptr):
	list_ids = []
	for line in file_ptr:
		if 'post' in line:
			continue
		postid = re.split('\n',line)[0]
		list_ids.append(postid)
	return list_ids

def check_ifUnique():
	file_lost = open('sandyspets-postid-lost.csv','r')
	file_found = open('sandyspets-postid-found.csv','r')
	file_unknown = open('sandyspets-postid-unknown.csv','r')
	
	list_lost = fileToList(file_lost)
	list_found = fileToList(file_found)
	list_unknown = fileToList(file_unknown)

	overlap = list_overlap(list_found,list_unknown)

	print "overlap: "+str(overlap)
	
def analyzeSpecificPosts():
	(cur,con) = getDBConnection('sandyspets6-8')
	#results = cur.execute("select post from post_info where postid in %s")
	csvfile = open('sandyspets-postid - unknown_type(2).csv','r')
	list_postid = []
	for line in csvfile:
		if 'post' in line:
			continue
		# postid = re.split("\"",re.split('\n',line)[0])[1]
		postid = re.split('\n',line)[0]
		list_postid.append(postid)
	post_ids='('+','.join(list_postid)+')'
	#print post_ids
	sql_string='select post from post_info where postid in '+post_ids+';'
	results = cur.execute(sql_string)
	list_posts = [(list(result)) for result in results]
	file_name = 'bow-unknown_type-posts(2).txt'
	post_frequencies = getFrequency(list_posts,file_name)

def analyzeMessages(type="POST"):
	(cur,con) = getDBConnection('sandyspets6-8')
	if type=="POST":
		results = cur.execute("select post from post_info;")
		file_name = 'post-freqdist.txt'

	elif type=="COMMENT":
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

def mapUsers():
	(cur,con) = getDBConnection('sandyspets6-8')
	results = cur.execute('select distinct userid from user_activities;')
	list_users = [result[0].encode('ascii','ignore') for result in results]
	user_num = cur.execute('select count(*) from user_mapping').next()[0]
	for user in list_users:
		user_num += 1
		cur.execute('insert into user_mapping values (?,?);',[user, user_num])
	con.commit()

def getTime(timestampstring):
	timeval= time.strptime(timestampstring,'%Y-%m-%d %H:%M:%S')
	return datetime.fromtimestamp(time.mktime(timeval))

def mapAllPetReportsFromDB(DB_NAME,type="PHOTO"):
	(cur,con) = getDBConnection(DB_NAME)
	if type == "PHOTO":
		sql_string = "select photo_id,created_time from photos_info;" 
	elif type == "POSTS":
		sql_string = "select post_id,created_time from post_info;"
	results = cur.execute(sql_string)
	list_petreports = [[result[0].encode('ascii','ignore'),result[1].encode('ascii','ignore')] for result in results]
	petreport_num = cur.execute('select count(*) from petreport_mapping').next()[0]
	for [petreport,created_time] in list_petreports:
		petreport_num += 1
		cur.execute('insert into petreport_mapping values (?,?,?,?);',[petreport, petreport_num, getTime(created_time), "Unknown"])
	con.commit()

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

#analyzeMessages(type="COMMENT")
# analyzeSpecificPosts()
#check_ifUnique()
# generatePetReports()
# getAlbumNameforPosts()
#mapUsers()
#generatePetReports(posts_file="sandyspets-photos-id.csv")
mapAllPetReportsFromDB('sandyspets6-8')