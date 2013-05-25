import config
import re, random
from analyze import getDBConnection
LOG_FORMAT = "%h %l %u %t \"%r\" %>s %b %{X-Remote-User-Name}o %{X-Remote-User-Id}o"
LOG_FILE = ""

occurrences = LOG_FORMAT.count('%')

def getFeatures(LOG_FILE,LOG_FORMAT):
	print 'Not Implemented'

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
					
def getPetreportsListFromDB():
	(cur,con) = getDBConnection('sandyspets6-8')
	try:
		results = cur.execute('select petreport_id from petreport_mapping;')
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)
	list_posts = [(result[0]) for result in results]
	return list_posts

def getUserListfromDB():
	(cur,con) = getDBConnection('sandyspets6-8')
	try:
		results = cur.execute('select user_id from users_mapping;')
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)
	list_users = [(result[0]) for result in results]
	return list_users

def getPetReportsLikesListfromDB():
	(cur,con) = getDBConnection('sandyspets6-8')
	try:
		results = cur.execute('select userid,postid from user_likes;')
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)
	list_postlikes = [(result[0]) for result in results]
	return list_postlikes	
	
def generatePetReportsGET():
	list_pets = getPetreportsListFromDB()
	list_users = getUserListfromDB()
	list_postlikes = getPetReportsLikesListfromDB()
	for user, petreport in list_postlikes:
		log_string = 			 
	print "PetReports generated!"