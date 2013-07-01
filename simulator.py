import re, random,sys
import sqlite3 as sql
from analyze import getDBConnection,fileToList
from time import  localtime, strftime,strptime

def getFeatures(LOG_FILE,LOG_FORMAT):
	print 'Not Implemented'

def generateTimestamp(timestamp="",type="apache"):
	#TIME format is [26/Sep/2012:10:43:04 -0500]
	if timestamp=="":
		return strftime("[%d/%b/%Y:%H:%M:%S +0700]", localtime())
	elif type == "apache":
		return strftime("[%d/%b/%Y:%H:%M:%S +0700]",strptime(timestamp,'%Y-%m-%d %H:%M:%S'))
	#TIME FORMAT is Thu Mar 14 00:38:14 2013
	elif type == "activity-log":
		return strftime("[%a %b %d %H:%M:%S %Y]",strptime(timestamp,'%Y-%m-%d %H:%M:%S'))
					
def getPetreportsListFromDB(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
	try:
		results = cur.execute('select fbpost_id,petreport_id,created_time from petreport_mapping;')
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)
	list_posts = dict([[result[0].encode('ascii','ignore'),[str(result[1]),result[2].encode('ascii','ignore')]] for result in results])
	print 'petreport list generated'
	return list_posts

def getUserListfromDB(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
	try:
		results = cur.execute('select fbuser_id,user_id from user_mapping;')
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)
	list_users = dict([[result[0].encode('ascii','ignore'),str(result[1])] for result in results])
	print 'user list generated'
	return list_users

'''Simulate Pet Report Views'''
#Pet Report Likes are get Requests
def getPetReportsLikesListfromDB(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
	try:
		results = cur.execute('SELECT distinct a.userid, b.petreport_id, b.created_time from user_likes a,petreport_mapping b where a.post_id=b.fbpost_id and post_id not in (select * from duplicate_posts)')
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)
	list_postlikes = [[result[0].encode('ascii','ignore'),result[1],result[2].encode('ascii','ignore')] for result in results]
	print 'user likes list generated'
	return list_postlikes	

# written to the apache server log file 	
def generatePetReportsViews(output_filename,DB_NAME):
	list_users = getUserListfromDB(DB_NAME)
	list_postlikes = getPetReportsLikesListfromDB(DB_NAME)
	output_file = open(output_filename,'w')
	for [user, petreport,timestamp] in list_postlikes:
		if user not in list_users:
			print 'user is not in list'
			continue
		user_id = list_users[user]
		log_string = generateLogString(user_id,petreport,"GET","petreport",timestamp)		
		print log_string	 
		output_file.write(log_string+'\n')
	print "PetReports GET Requests generated!"

'''Pet Report Submission Simulation'''

def getSubmittedPetReportsListfromDB(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME) 
	try:
		#page_id = cur.execute('select author_id,count(author_id) as countid from post_info group by author_id order by countid desc limit 1;')[0].encode('ascii','ignore')
		results = cur.execute('SELECT distinct a.author_id,b.fbpost_id from post_info a, userpost_mapping b where b.userpostid=a.postid;')
		list_submittedpetreports =  [[result[0].encode('ascii','ignore'),result[1].encode('ascii','ignore')] for result in results]
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)	
	return list_submittedpetreports

#Writes to Activity Log file of all users	
#Wed Mar 13 10:31:57 2013 [PETREPORT_SUBMITTED]: amrutha submitted the PetReport for {XYZ} with ID{823}
def generatePetReportsSubmission(output_filename,DB_NAME):
	#all-posts(?) mapped
	list_pets = getPetreportsListFromDB(DB_NAME)
	#post-authors
	list_users = getUserListfromDB(DB_NAME)
	#user-posts
	list_userposts = getSubmittedPetReportsListfromDB(DB_NAME)
	for [user, petreport] in list_userposts:
		if user not in list_users:
			print 'user is not in list'
			continue
		if petreport not in list_pets:
			print 'pet report onot in list'
			continue
		user_id = list_users[user]
		pet_id = list_pets[petreport][0]
		timestamp = list_pets[petreport][1]
		log_string = generateLogString(user_id,pet_id,"POST","PETREPORT_SUBMITTED",timestamp,type="activity-log")		
		output_file = open(output_filename+str(user_id)+".txt",'a+')
		output_file.write(log_string+'\n')
		print log_string	 
	print "PetReports POST Requests generated!"

'''Simulate Pet Report Bookmarking'''

def getPetReportsCommentersListfromDB(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
	try:
		#change: exclude pet match creators
		#select count(*) from (SELECT userid,post_id,a.created_time from user_comments a,post_info b where post_id not in (select * from duplicate_posts) and post_id=postid and b.author_id="403922739676650");
		results = cur.execute('SELECT distinct userid,post_id,a.created_time from user_comments a,post_info b where post_id not in (select * from duplicate_posts) and post_id=postid and b.author_id="403922739676650"')
		list_postcommenters = [[result[0].encode('ascii','ignore'),result[1].encode('ascii','ignore'),result[2]] for result in results]
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)
	print 'Commenters-Post list generated'
	return list_postcommenters

#write to activity logs
#Mon May 20 10:16:27 2013 [PETREPORT_ADD_BOOKMARK]: amrutha has added a PetReport bookmark for {Rabbi} with ID{1452}
#Date<sp>[action]:<sp><username><string representign action><ID{ID#}>
def generatePetReportBookmarks(output_filename,DB_NAME):
	list_pets = getPetreportsListFromDB(DB_NAME)
	list_users = getUserListfromDB(DB_NAME)
	list_postcommenters = getPetReportsCommentersListfromDB(DB_NAME)
	for [user, petreport,timestamp] in list_postcommenters:
		if user not in list_users:
			continue
		if petreport not in list_pets:
			continue
		user_id = list_users[user]
		pet_id = list_pets[petreport][0]
		log_string = generateLogString(user_id,pet_id,"POST","PETREPORT_ADD_BOOKMARK",timestamp,type="activity-log")		
		try:
			output_file = open(output_filename+user_id+".txt",'a+')
		except:
			output_file = open(output_filename+user_id+".txt",'w')
		print log_string	 
		output_file.write(log_string+'\n')
	print "PetReports bookmarks POST Requests generated!"

'''Simulation of Pet Match Creation'''

#comments that suggest matching activity
def getPetMatchListsFromDB(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
	try:
		results = cur.execute('SELECT distinct a.commentid,a.post_id,a.userid,a.created_time FROM user_comments a,petmatch_mapping b WHERE a.commentid=b.fbcomment_id and a.post_id not in (select post_id from duplicate_posts);')
		list_petmatchcomments = [[result[0].encode('ascii','ignore'), result[1].encode('ascii','ignore'), result[2].encode('ascii','ignore'), result[3].encode('ascii','ignore')] for result in results]
		results = cur.execute ('SELECT fbcomment_id,petmatch_id from petmatch_mapping;')
		list_petmatchmaps = dict([[result[0].encode('ascii','ignore'), result[1]] for result in results])
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)	
	return (list_petmatchcomments,list_petmatchmaps)

#writes apache server log and user activity logs 
#Tue May  7 22:12:36 2013 [PETMATCH_PROPOSED]: amrutha proposed the PetMatch object with ID{467}
#users proposing pet matches based on matching suggestions seen on the facebook data
def generatePetMatchCreate(output_filename,activity_file,DB_NAME):
	list_users = getUserListfromDB(DB_NAME)
	(list_petmatchcomments,list_petmatchmaps) = getPetMatchListsFromDB(DB_NAME)
	list_pets = getPetreportsListFromDB(DB_NAME)
	output_file = open(output_filename,'a+')
	temp_out_file = open("temp_out_file","w")
	for [commentid,petreport,user,timestamp] in list_petmatchcomments:
		if user not in list_users:
			print 'user %s not in list_users' %(user)
			continue
		if petreport not in list_pets:
			print 'pet %s not in list_pets' %(petreport)
			temp_out_file.write('pet %s not in list_pets\n' %(petreport))
			continue
		if petreport+'MATCH' not in list_pets:
			print 'pet %s not in list_pets' %(petreport+'MATCH')
			temp_out_file.write('pet %s not in list_pets\n' %(petreport+'MATCH'))
			continue
		user_id = list_users[user]
		pet1_id = list_pets[petreport][0]
		pet2_id = list_pets[petreport+'MATCH'][0]
		petmatch_id = list_petmatchmaps[commentid]
		log_string = generateLogString(user_id,str(pet1_id)+'/'+str(pet2_id),"POST","propose_petmatch",timestamp)
		print log_string	 
		output_file.write(log_string+'\n')
		output_file_activities = open(activity_file+user_id+".txt","a+")
		activity_log_string = generateLogString(user_id,petmatch_id,"POST","PETMATCH_PROPOSED", timestamp, type="activity-log")
		print activity_log_string
		output_file_activities.write(activity_log_string+'\n')

	print "PetMatch POST Requests generated!"

'''Simulation of Pet Match Views'''
#get list of pet match viewers from a specific table
def getPetMatchViewsListfromDB(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
	try:
		results = cur.execute('SELECT DISTINCT b.fbcomment_id_petmatch, a.userid, a.created_time FROM user_comments a, user_comments_petmatchviews b where a.commentid = b.fbcomment_id_viewer;')
		list_petmatchviews = [[result[0].encode('ascii','ignore'), result[1].encode('ascii','ignore'), result[2].encode('ascii','ignore')] for result in results]
		results = cur.execute ('SELECT distinct fbcomment_id,petmatch_id from petmatch_mapping;')
		list_petmatchmaps = dict([[result[0].encode('ascii','ignore'), result[1]] for result in results])
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)	
	return (list_petmatchviews,list_petmatchmaps)	

#petmatchviews_filename is a table in the DB with a list of commentids and users who have viewed them(?)
def generatePetMatchViews(output_filename, DB_NAME):
	list_users = getUserListfromDB(DB_NAME)
	(list_petmatchviews,list_petmatchmaps) = getPetMatchViewsListfromDB(DB_NAME)
	output_file = open(output_filename,'a+')
	#need list of users with petmatch that they have viewed and the time of viewing. this would
	#possibly be stored in a table. the table would have details about petmatch-comment-id, petmatch
	#viewing timestamp, user-id, 
	for [commentid,user,timestamp] in list_petmatchviews:
		if user not in list_users:
			print 'user %s not in list_users' %(user)
			continue
		if commentid not in list_petmatchmaps:
			print 'petmatch %s not in list_petmatches' %(petmatch)
			continue
		user_id = list_users[user]
		petmatch_id = list_petmatchmaps[commentid]
		log_string = generateLogString(user_id,petmatch_id,"GET","petmatch",timestamp)
		print log_string	 
		output_file.write(log_string+'\n')
	print "PetMatch GET Requests generated!"

'''Simulation of Pet Match voting'''
#UNTESTED
def getPetMatchVotersListfromDB(DB_NAME):
	#need list of users with petmatch that they have voted and the time of voting. this would
	#possibly be stored in a table. the table would have details about petmatch-comment-id, petmatch
	#voting timestamp, user-id, 
	(cur,con) = getDBConnection(DB_NAME)
	try:
		results = cur.execute('SELECT distinct fbcomment_id_petmatch, fbuser_id, vote_time FROM user_comments_petmatchvotes')
		list_petmatchvoters = [[result[0].encode('ascii','ignore'), result[1].encode('ascii','ignore'), result[2].encode('ascii','ignore')] for result in results]
		results = cur.execute ('SELECT fbcomment_id,petmatch_id from petmatch_mapping;')
		list_petmatchmaps = dict([[result[0].encode('ascii','ignore'), result[1]] for result in results])
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)	
	return (list_petmatchvoters,list_petmatchmaps)	

#write to activity log
#Mon May 20 10:17:47 2013 [PETMATCH_UPVOTE]: amrutha upvoted the PetMatch object with ID{520}
#Thu Mar 14 00:38:14 2013 [PETMATCH_DOWNVOTE]: amrutha downvoted the PetMatch object with ID{216}
#Date<sp>[action]:<sp><username><sp><string representign action><ID{ID#}>
def generatePetMatchVote(output_filename, DB_NAME):
	list_users = getUserListfromDB(DB_NAME)
	(list_petmatchvoters,list_petmatchmaps) = getPetMatchVotersListfromDB(DB_NAME)
	for [commentid,user,timestamp] in list_petmatchvoters:
		if user not in list_users:
			print 'user %s not in list_users' %(user)
			continue
		if commentid not in list_petmatchmaps:
			print 'petmatch %s not in list_petmatches' %(petmatch)
			continue
		user_id = list_users[user]
		petmatch_id = list_petmatchmaps[commentid]
		vote_type = random.randint(1,2)
		if vote_type == 1:
			request = "PETMATCH_UPVOTE"
		else:
			request = "PETMATCH_DOWNVOTE"
		log_string = generateLogString(user_id,petmatch_id,'POST',request,timestamp,type="activity-log")
		
		output_file = open(output_filename+str(user_id)+".txt",'a')
		output_file.write(log_string+'\n')
		print log_string
		#factor in the activity logs. either in the form of mongodb or text files
	print "Vote PetMatch POST Requests generated!"

def generateLogString(userid,objectid,method,request_object,timestamp="",type="apache"):
	LOG_LOOKUP = {"%h":"IPADDR","%l":"RFCID","%u":"HTTPUSER","%t":"TIME","\"%r\"":"REQSTRING","%>s":"STATUSCODE","%b":"OBJSIZE","%{X-Remote-User-Name}o":"USERNAME","%{X-Remote-User-Id}o":"USERID"}
	LOG_FORMAT = "%h %l %u %t \"%r\" %>s %b %{X-Remote-User-Name}o %{X-Remote-User-Id}o"
	VALUE_LOOKUP = {"%h":"127.0.0.1","%l":"-","%u":"-","%>s":"200","%b":""}
	URL = {"petreport":"/reporting/PetReport/","bookmark":"/reporting/bookmark/","propose_petmatch":"/matching/propose_petmatch/","petmatch":"/matching/petmatch/","vote_petmatch":"/matching/vote_petmatch/"}
	ACTIVITY_STRING = {"PETREPORT_ADD_BOOKMARK":"has added a PetReport bookmark for pet with ID","PETMATCH_UPVOTE":"upvoted the PetMatch object with ID","PETMATCH_DOWNVOTE":"downvoted the PetMatch object with ID","PETMATCH_PROPOSED":"proposed the PetMatch object with ID","PETREPORT_SUBMITTED":"submitted the PetReport with ID"}
	log_elements = LOG_FORMAT.split()
	log_string = ""
	if type == "apache":
		for element in log_elements:
			if element in VALUE_LOOKUP:
				log_string += (VALUE_LOOKUP[element]+" ")
			elif element == "%t":
				if request_object in URL:
					log_string += (generateTimestamp(timestamp)+" ")
				else:
					log_string += (generateTimestamp()+" ")
			elif element == "\"%r\"":
				log_string += ("\""+method+" "+URL[request_object]+str(objectid)+" HTTP/1.0\" ")
			elif element == "%{X-Remote-User-Name}o":
				log_string += ("user"+str(userid)+" ")
			elif element == "%{X-Remote-User-Id}o":
				log_string += (str(userid))
	else:
		#Date<sp>[action]:<sp><username><string representing action><ID{ID#}>
		log_string = generateTimestamp(timestamp,type)+" "+"["+request_object+"]"+" "+"user"+str(userid)+" "+ACTIVITY_STRING[request_object]+"{"+str(objectid)+"}"
	return log_string

DB_NAME="sandyspets530628.db"
# generatePetReportsViews("simulated-apachelogs/apache_access.log",DB_NAME)
# generatePetMatchCreate("simulated-apachelogs/apache_access.log","simulated-activity-logs/",DB_NAME)
# generatePetReportsSubmission("simulated-activity-logs/",DB_NAME)
# generatePetMatchViews("simulated-apachelogs/apache_access.log", DB_NAME)
# generatePetMatchVote("simulated-activity-logs/",DB_NAME)
# generatePetReportBookmarks("simulated-activity-logs/",DB_NAME)