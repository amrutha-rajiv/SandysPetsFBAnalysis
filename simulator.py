import config
import re
LOG_FORMAT = "%h %l %u %t \"%r\" %>s %b %{X-Remote-User-Name}o %{X-Remote-User-Id}o"
LOG_FILE = ""

occurrences = LOG_FORMAT.count('%')

def getFeatures(LOG_FILE,LOG_FORMAT):


def writeLog(LOG_FILE,LOG_FORMAT,ACTIVITY,USER,TIMESTAMP):
	try:
		log_writer = open(LOG_FILE,'a+')
	except:
		log_writer = open(LOG_FILE,'w')
	
	# variables = re.split(' ',LOG_FORMAT)
	# num_variables = LOG_FORMAT.count('%')
	# for var in variables:

	# 	if var== '\"%r\"' or var== '%r':
	# 		log_writer.write(URLS[ACTIVITY])
	# 	elif 
	log_writer.write('- - - '+TIMESTAMP+' '+ACTIVITY+' - - '+USER['ID']+' '+USER['NAME']+'\n')
					

