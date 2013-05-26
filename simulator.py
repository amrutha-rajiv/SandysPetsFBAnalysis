import re, random,sys
import sqlite3 as sql
from analyze import getDBConnection, getTime
from time import  localtime, strftime


def getFeatures(LOG_FILE,LOG_FORMAT):
	print 'Not Implemented'

def generateRandomTimestamp():
	#TIME format is [26/Sep/2012:10:43:04 -0500]
	return strftime("[%d/%b/%Y:%H:%M:%S +0700]", localtime())

def writeLog(LOG_FILE,LOG_FORMAT,ACTIVITY,USER,TIMESTAMP):
	try:
		log_writer = open(LOG_FILE,'a+')
	except:
		log_writer = open(LOG_FILE,'w')
	petreports = getPetreporsListFromDB()
	# variables = re.split(' ',LOG_FORMAT)
	# num_variables = LOG_FORMAT.count('%')
	# for var in variables:

	# 	if var== '\"%r\"' or var== '%r':
	# 		log_writer.write(URLS[ACTIVITY])
	# 	elif 
	log_writer.write('- - - '+TIMESTAMP+' '+ACTIVITY+' - - '+USER['ID']+' '+USER['NAME']+'\n')
					
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

def getPetReportsLikesListfromDB(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
	try:
		results = cur.execute('select userid,photo_id from user_likes_photos;')
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)
	list_postlikes = [[result[0].encode('ascii','ignore'),result[1].encode('ascii','ignore')] for result in results]
	print 'user likes list generated'
	return list_postlikes	
	
def generatePetReportsGET(output_filename,DB_NAME):
	list_pets = getPetreportsListFromDB(DB_NAME)
	list_users = getUserListfromDB(DB_NAME)
	list_postlikes = getPetReportsLikesListfromDB(DB_NAME)
	output_file = open(output_filename,'w')
	for [user, petreport] in list_postlikes:
		if user not in list_users:
			continue
		if petreport not in list_pets:
			continue
		user_id = list_users[user]
		pet_id = list_pets[petreport][0]
		timestamp = list_pets[petreport][1]
		log_string = generateLogString(user_id,pet_id,"GET","petreport",timestamp)		
		print log_string	 
		output_file.write(log_string+'\n')
	print "PetReports GET Requests generated!"

def generateLogString(userid,objectid,method,request_object,timestamp=""):
	LOG_LOOKUP = {"%h":"IPADDR","%l":"RFCID","%u":"HTTPUSER","%t":"TIME","\"%r\"":"REQSTRING","%>s":"STATUSCODE","%b":"OBJSIZE","%{X-Remote-User-Name}o":"USERNAME","%{X-Remote-User-Id}o":"USERID"}
	LOG_FORMAT = "%h %l %u %t \"%r\" %>s %b %{X-Remote-User-Name}o %{X-Remote-User-Id}o"
	VALUE_LOOKUP = {"%h":"127.0.0.1","%l":"-","%u":"-","%>s":"200","%b":""}
	URL = {"petreport":"/reporting/PetReport/"}
	log_elements = LOG_FORMAT.split()
	log_string = ""
	for element in log_elements:
		if element in VALUE_LOOKUP:
			log_string += (VALUE_LOOKUP[element]+" ")
		elif element == "%t":
			if request_object == "petreport":
				log_string += (getTime(timestamp)+" ")
			else:
				log_string += (generateRandomTimestamp()+" ")
		elif element == "\"%r\"":
			log_string += ("\""+method+" "+URL[request_object]+objectid+" HTTP/1.0\" ")
		elif element == "%{X-Remote-User-Name}o":
			log_string += ("user"+str(userid)+" ")
		elif element == "%{X-Remote-User-Id}o":
			log_string += (str(userid))
	return log_string

generatePetReportsGET("test_get.log",'sandyspets6-8')
