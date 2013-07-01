#python libraries
import sys, time,re,random
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

#create mapping tables for mapping all objects
def createMappingTables(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
	cur.execute('CREATE TABLE IF NOT EXISTS petreport_mapping(fbpost_id TEXT, petreport_id INT, created_time TIMESTAMP, pet_status TEXT);')
	cur.execute('CREATE TABLE IF NOT EXISTS user_mapping(fbuser_id TEXT, user_id INT);')
	cur.execute('CREATE TABLE IF NOT EXISTS petmatch_mapping(fbcomment_id TEXT, petmatch_id INT);')		
	cur.execute('CREATE TABLE IF NOT EXISTS userpost_mapping(fbpost_id TEXT, userpostid TEXT);')		
	cur.execute('CREATE TABLE IF NOT EXISTS user_comments_petmatchviews(fbcomment_id_petmatch TEXT, fbcomment_id_viewer TEXT);')
	cur.execute('CREATE TABLE IF NOT EXISTS user_comments_petmatchvotes(fbcomment_id_petmatch TEXT,fbuser_id TEXT,vote_time TIMESTAMP)')

#return overlapping values between 2 lists
def list_overlap(list_a,list_b):
	return any(i for i in list_a if i in list_b)

def getTime(timestampstring):
	timeval= time.strptime(timestampstring,'%Y-%m-%d %H:%M:%S')
	return datetime.fromtimestamp(time.mktime(timeval))

#convert a file with a single column to a list
def fileToList(file_ptr):
	list_ids = []
	for line in file_ptr:
		if 'post' in line or 'comment' in line:
			continue
		postid = re.split('\n',line)[0]
		list_ids.append(postid)
	return list_ids

#check if pets between lost/found/unknown files overlap/intersect
def check_ifUnique():
	file_lost = open('sandyspets-postid-lost.csv','r')
	file_found = open('sandyspets-postid-found.csv','r')
	file_unknown = open('sandyspets-postid-unknown.csv','r')
	
	list_lost = fileToList(file_lost)
	list_found = fileToList(file_found)
	list_unknown = fileToList(file_unknown)

	overlap = list_overlap(list_found,list_unknown)

	print "overlap: "+str(overlap)

#Get word frequencies from list strings
def getFrequency(list,output_file_name):
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
	out_file = open(output_file_name,'w')
	for element in fd.keys():
		if fd[element] > 1:
			print (element.encode('utf8')+" "+str(fd[element]))			
			out_file.write(element.encode('utf8')+" "+str(fd[element])+"\n")

def getPostLabels(DB_NAME,file_name):
	#cannot match post with photo using ID, a message or smtg is reqd
	album_status={"Needs Foster or Adoption":"FOUND/ADOPT" , "NJ - FOUND CATS":"FOUND", "Their Journeys":"Journeys",\
	"NJ - LOST CATS":"LOST","NY - FOUND DOGS":"FOUND","NY - LOST DOGS":"LOST", "NY - FOUND CATS":"FOUND", \
	"NY - LOST CATS" :"LOST","NJ - LOST DOGS":"LOST", "NJ - FOUND DOGS":"FOUND",\
	"BrooklynACC Emergency Center":"UKNOWN",\
	"SAFE":"UNKNOWN","Missing Pieces":"UNKNOWN","Profile Pictures":"SKIP",\
	"Rainbow Bridge - RIP":"RIP", "In Temporary Foster":"FOUND","Adopted":"FOUND","NY and NJ BIRDS":"UNKNOWN",\
	"HELPFUL TIPS":"SKIP","Mitchell Field Emergency Shelter":"FOUND","Cover Photos":"SKIP","Timeline Photos":"UKNOWN",'DISPLACED Sandy Pets - NEEDING HOMES':'FOUND/ADOPT','REUNITED  Happy Endings':"REUNITED",'Connecticut  Pennsylvania  Maryland  Massachusetts':"unknown",'In Temporary Foster or with Rescue in a Kennel':"FOUND"}
	counters = {}
	list_postids = fileToList(open(file_name,'r'))
	list_postids = '('+','.join(list_postids)+')'
	#print list_postids
	(cur,con) = getDBConnection(DB_NAME)	
	try:
		results = cur.execute('select album_name from post_info_new a,photos_info b where object_id=photo_id and postid in '+list_postids)
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)
	album_names = [result[0].encode('ascii','ignore') for result in results]
	print "num_Statuses: "+str(len(album_names))
	for album_name in album_names:
		pet_status = album_status[album_name]
		if pet_status in counters:
			counters[pet_status] +=1
		else:
			counters[pet_status]=1

	out_file = open('PagePostUknownstatus-counts.txt','w')
	for key in counters.keys():
		print "%s:%d" %(key,counters[key])
		out_file.write("%s:%d\n" %(key,counters[key]))

#Get frequent words from posts with specific postids
def analyzeSpecificPosts(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
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

#get word frequencies from all posts/all comments
def analyzeMessages(DB_NAME,output_file_name,type="POST"):
	(cur,con) = getDBConnection(DB_NAME)
	if type=="POST":
		results = cur.execute("select distinct post from post_info;")
	elif type=="COMMENT":
		results = cur.execute("select comment from user_comments;")
	else:
		print 'invalid type'
		sys.exit()
	#compile the tuple of posts as a list of posts in python
	list_posts = [(list(result)) for result in results]
	post_frequencies = getFrequency(list_posts,output_file_name)


'''Mapping of EPM Objects from Facebook posts'''

#map pet reports with ids in file posts_file
#unused function
def generatePetReports(DB_NAME,posts_file="sandyspets-postid-lost.csv",pet_status="LOST"):
	(cur,con) = getDBConnection(DB_NAME)
	petreport_num = cur.execute('select count(*) from petreport_mapping').next()[0]
	list_postid = fileToList(open(posts_file,'r'))
	allpostids = '('+','.join(list_postid)+')'
	for postid in csvfile:
		if 'post' in line:
			continue
		postid = re.split('\n',line)[0]
		list_postid.append(postid)
		petreport_num += 1
		cur.execute('insert into petreport_mapping values (?,?,?)',[postid,petreport_num,pet_status])
	con.commit()

#map all users
def mapUsers(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
	results = cur.execute('select distinct userid from (select distinct userid from user_likes union select distinct userid from user_comments union select distinct userid from user_likes);')
	list_users = [result[0].encode('ascii','ignore') for result in results]
	#get existing users
	results = cur.execute('select fbuser_id from user_mapping;')
	existing_users = [result[0].encode('ascii','ignore') for result in results]
	user_num = len(existing_users)
	for user in list_users:
		if user not in existing_users:
			user_num += 1
			cur.execute('insert into user_mapping values (?,?);',[user, user_num])
			existing_users.append(user)
	con.commit()

#map all posts from post_info, photos_info
def mapAllPetReports(DB_NAME,petreport_filename,pet_status,type="POSTS"):
	(cur,con) = getDBConnection(DB_NAME)
	list_petreports = fileToList(open(petreport_filename,'r'))
	list_petreports='('+','.join(list_petreports)+')'
	if type == "PHOTO":
		sql_string = "select photo_id,created_time from photos_info;" 
	elif type == "POSTS":
		sql_string = "select postid,created_time from post_info where author_id = \"403922739676650\" and postid in "+list_petreports+"and postid not in (SELECT POST_ID FROM DUPLICATE_POSTS);"
	results = cur.execute(sql_string)
	list_petreports = [[result[0].encode('ascii','ignore'),result[1].encode('ascii','ignore')] for result in results]
	#get existing petereports
	results = cur.execute('select fbpost_id from petreport_mapping;')
	existing_posts = [result[0].encode('ascii','ignore') for result in results]
	petreport_num = len(existing_posts)
	for [petreport,created_time] in list_petreports:
		if petreport not in existing_posts:
			petreport_num += 1
			cur.execute('insert into petreport_mapping values (?,?,?,?);',[petreport, petreport_num, getTime(created_time), pet_status])
			existing_posts.append(petreport)
	con.commit()

def mapPetmatches(DB_NAME,petmatch_filename):
	#get all pet match comments
	comments_list = fileToList(open(petmatch_filename,'r'))
	comments_list='('+','.join(comments_list)+')'
	(cur,con) = getDBConnection(DB_NAME)
	#get existing petreports
	results = cur.execute('SELECT fbpost_id from petreport_mapping;')
	existing_petreports = [result[0].encode('ascii','ignore') for result in results]
	petreport_num = len(existing_petreports)
	#get existing petmatches
	#correct this statement
	results = cur.execute('SELECT fbcomment_id from petmatch_mapping;')
	existing_matches = [result[0].encode('ascii','ignore') for result in results]
	petmatch_num = len(existing_matches)
	#get petmatch comments
	results = cur.execute('SELECT commentid,post_id from user_comments where commentid in '+comments_list)
	#iterate over the list of comments to identify a mapped postid for every post
	list_petmatchcomments = [[result[0].encode('ascii','ignore'), result[1].encode('ascii','ignore')] for result in results]
	for [commentid,postid] in list_petmatchcomments:
		
		pet2_id = postid+"MATCH"
		if  commentid not in existing_matches:	
			petmatch_num += 1
			cur.execute('INSERT INTO petmatch_mapping VALUES(?,?)',[commentid,petmatch_num])
			existing_matches.append(commentid)
			
		if pet2_id not in existing_petreports:
			petreport_num += 1
			cur.execute('INSERT INTO petreport_mapping VALUES(?,?,?,?)',[pet2_id,petreport_num,"","Unknown"])
			existing_petreports.append(pet2_id)
			
		if postid not in existing_petreports:
			created_time = cur.execute("SELECT created_time from post_info where postid=\""+postid+"\"").next()[0]
			petreport_num += 1
			cur.execute('INSERT INTO petreport_mapping VALUES(?,?,?,?)',[postid,petreport_num,getTime(created_time),"Unknown"])
			existing_petreports.append(postid)
			
	con.commit()

#map all posts posted by users 
def mapUserPosts(DB_NAME,userpost_filename,pet_status):

	userpost_file = open(userpost_filename,'r')
	list_userpostids = fileToList(userpost_file)
	(cur,con) = getDBConnection(DB_NAME)
	results = cur.execute("SELECT distinct fbpost_id from userpost_mapping;")
	mapped_posts = [("\""+result[0].encode('ascii','ignore')+"\"") for result in results]
	mapped_posts = '('+','.join(mapped_posts)+')'
	if pet_status == "unknown":
		query = "SELECT distinct fbpost_id from petreport_mapping except select distinct fbpost_id from userpost_mapping;"
	else:
		query = "SELECT distinct fbpost_id from petreport_mapping where pet_status=\""+pet_status+"\" or pet_status=\"Unknown\" and fbpost_id not in "+mapped_posts

	results = cur.execute(query)	
	unmapped_posts = [result[0].encode('ascii','ignore') for result in results]
	for userpostid in list_userpostids:
		userpostid= re.split("\"",userpostid)[1]
		try:
			fbpost_id = random.choice(unmapped_posts)
		except IndexError:
			break
		cur.execute("INSERT INTO userpost_mapping values(?,?)",[fbpost_id,userpostid])
		unmapped_posts.remove(fbpost_id)

	con.commit()	
	print "[info] successfully mapped all user posts with status %s." %(pet_status)

def mapPetMatchViews(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
	# results = cur.execute("SELECT distinct fbcomment_id_viewer from user_comments_petmatchviews;")#fbcommentid_petmatch
	# #constructing the views table requires us to analyze other comments in association with matching comments. select distinct post_id,count(commentid) from user_comments where post_id in (select distinct post_id from user_comments a, petmatch_mapping b where a.commentid=b.fbcomment_id) and post_id not in (select post_id from duplicate_posts) group by post_id order by count(commentid) desc limit 10;
	results = cur.execute("SELECT distinct post_id,commentid from user_comments where post_id in (select distinct post_id from user_comments a, petmatch_mapping b where a.commentid=b.fbcomment_id and post_id not in (select post_id from duplicate_posts)) order by post_id;")
	list_comments = {}
	i=0
	for (postid,commentid) in results:
		postid = postid.encode('ascii','ignore')
		commentid = commentid.encode('ascii','ignore')
		#print postid+ "|"+commentid
		i += 1
		print str(i)
		if postid not in list_comments:
			list_comments[postid] = [commentid]
		else:
			list_comments[postid].append(commentid)
	#print '[DEBUG]: '+str(list_comments)
	i=0
	row=0
	results = cur.execute("SELECT distinct a.commentid,a.post_id from user_comments a, petmatch_mapping b where a.commentid=b.fbcomment_id and a.post_id not in (select post_id from duplicate_posts);")
	list_petmatchcomments = [[result[0].encode('ascii','ignore'), result[1].encode('ascii','ignore')] for result in results]
	for [commentid,postid] in list_petmatchcomments:
		row +=1
		print str(row)
		for fbcomment_id_viewer in list_comments[postid]:
			i += 1
			print str(i)
			cur.execute("INSERT into user_comments_petmatchviews values(?,?)",[commentid.encode('ascii','ignore'),fbcomment_id_viewer])
	con.commit()

	print "[info] successfully mapped all pet match views"

#notes  results = cur.execute("SELECT a.userid,a.commentid,a.post_id from user_comments a, user_likes b where a.userid=b.userid and a.post_id=b.post_id and commentid in "+list_commentids2)
#
def mapPetMatchVoters(DB_NAME,petmatchvotes_filename):
	list_petmatchvotercomments = fileToList(open(petmatchvotes_filename,'r'))	
	list_petmatchvotercomments = '('+','.join(list_petmatchvotercomments)+')'
	#print str(list_petmatchvotercomments)
	(cur,con) = getDBConnection(DB_NAME)
	#retrieve voters' user name, related comment, comment timestamp and post ID 
	results = cur.execute("SELECT distinct a.commentid, a.created_time, a.post_id , a.userid from user_comments a, user_likes b where a.userid=b.userid and a.post_id=b.post_id and commentid not in (select fbcomment_id from petmatch_mapping ) and commentid in  "+list_petmatchvotercomments)
	list_petmatchvotercomments = []
	list_petmatchvotercomments = [[result[0].encode('ascii','ignore'), result[1].encode('ascii','ignore'), result[2].encode('ascii','ignore'), result[3].encode('ascii','ignore')] for result in results]
	results = cur.execute('SELECT distinct post_id,commentid,created_time from user_comments where commentid in (select fbcomment_id from petmatch_mapping) and post_id not in (select * from duplicate_posts);')
	list_matches = dict([[result[0].encode('ascii','ignore'), [result[1].encode('ascii','ignore'), result[2].encode('ascii','ignore')]] for result in results])
	existing_votes = []
	for [commentid,created_time,postid,userid] in list_petmatchvotercomments:
		list_posts = list_matches.keys()
		if postid not in list_matches:
			match_post = random.choice(list_posts)
		else:
			match_post = postid
		while [list_matches[match_post][0],userid] in existing_votes and len(list_posts)>1:
			list_posts.remove(match_post)
			match_post = random.choice(list_posts)
		if len(list_posts)==1:
			continue
		vote_time = max(getTime(list_matches[match_post][1]), getTime(created_time))
		cur.execute('INSERT INTO user_comments_petmatchvotes values(?,?,?)',[list_matches[match_post][0],userid,vote_time])
		existing_votes.append([list_matches[match_post][0],userid])
	con.commit()		
	print "[info] successfully mapped all pet match votes"

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
# mapAllPetReportsFromDB('sandyspets6-8')
DB_NAME = 'sandyspets530628.db'

'''identify post labels'''
#getPostLabels(DB_NAME,"sandyspets530-postid-unknownpageposts.csv")

'''create all mapping tables'''
#createMappingTables(DB_NAME)

#analyzeMessages(DB_NAME,'Posts-BagOfWords.txt')

# mapUsers(DB_NAME)
# print ' User Mapping Complete'
# petreportfiles = [["sandyspets-specifics/sandyspets530-postid-lostposts.csv","lost"],["sandyspets-specifics/sandyspets530-postid-foundNadoptedposts.csv","found"]]
# for [file_name,pet_status] in petreportfiles:
# 	mapAllPetReports(DB_NAME,file_name, pet_status,type="POSTS")
# print 'PetReport Mapping Complete'
# crosspostfiles = [["sandyspets-specifics/sandyspets530-postid-crosspostedlostpets.csv","lost"],["sandyspets-specifics/sandyspets530-postid-crosspostedfoundpets.csv","found"],["sandyspets-specifics/sandyspets530-postid-craigslist.csv","unknown"]]
# for [file_name,pet_status] in crosspostfiles:
# 	mapUserPosts(DB_NAME,file_name,pet_status)
# print 'UserPost Mapping Complete'
# mapPetmatches(DB_NAME,"sandyspets-specifics/sandyspets530-commentid-matches.csv")
# print 'PetMatch Mapping Complete'
# mapPetMatchViews(DB_NAME)
# print 'Petmatch views Mapping Complete'
# mapPetMatchVoters(DB_NAME,'sandyspets-specifics/sandyspets530-commentid-shared_comments.csv')
# print 'PetMatch Voters Mapping complete'
