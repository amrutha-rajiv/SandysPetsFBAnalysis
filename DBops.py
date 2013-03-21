#python libraries
import sys, time
from datetime import datetime
#third party libraries
import sqlite3 as sql
import requests, simplejson
from facepy import GraphAPI

#GLOBAL VARIABLES

ACCESS_TOKEN =raw_input("Enter a valid Facebook access token: ")
num_userposts = 1
num_useractivities = 1
num_userlikes = 1
num_posts = 0


def getPostsList():
	
	con = None
	postid_list = []

	try:
		con = sql.connect('sandyspets')
		cur = con.cursor()
		cur.execute("SELECT objectid from objects_list;")
		postid_list = [tuple[0] for tuple in cur]
	except sql.Error, e:
	    print "Error %s:" % e.args[0]
	    sys.exit(1)
	    
	finally: 
	    if con:
	        con.close()
	return postid_list

def getNewPosts():
	
	global ACCESS_TOKEN

	allposts = getPostsList()
	graph = GraphAPI(ACCESS_TOKEN)
	response = graph.get('sandyspets/?fields=posts.fields(id)')
	next_page  = response['posts']['paging']['next']
	response = response['posts']
	out_file = open('test1.txt','w')	
	while next_page!= "":	
		for element in response['data']:
	  		if element['id'] not in allposts:
  				insertPostinDB(getPost(element['id']))
	  	response = requests.get(next_page)
		if response.status_code == 200:
			try:
				#if 'next' in response.json()['paging'].keys():
				next_page = response.json()['paging']['next']
			except:
				next_page = ""
				print 'response: '+str(response.json())
   			response = response.json()		
   			

def getPost(postid):

	global ACCESS_TOKEN

	graph = GraphAPI(ACCESS_TOKEN)
	return graph.get(postid)
	
def insertPostinDB(post,deleted_flag=False):
	
	global num_userposts
	global num_useractivities
	global num_userlikes
	global num_posts

	postid = post["id"]
	num_posts  += 1
	if "from" in post.keys():
		author = [post["from"]["id"],post["from"]["name"]]
		# if author not in post_authors:
		# 	post_authors.append(author)
	
	if "message" in post.keys():
		message = post["message"]
	else:
		message = ""
	
	if "shares" in post.keys():
		shares_count = post["shares"]["count"]
	else:
		shares_count = 0
	
	if "likes" in post.keys():
		likes_count = post["likes"]["count"]
	else:
		likes_count = 0
	
	if 'link' in post.keys():
		link = post["link"]
	else:
		link = ""

	con = sql.connect('sandyspets')
	cur = con.cursor()

	cur.execute('insert into post_info values(?,?,?,?,?,?,?,?,?,?)',[postid, message, author[0],\
		author[1],shares_count, likes_count,post["type"],link,getTime(post["created_time"]),\
		getTime(post["updated_time"])])
	print 'inserted into post_info table [ %s, %s, %s, %s, %d, %d,%s,%s,%s,%s]' %(postid, message, author[0],\
		author[1],shares_count, likes_count,post["type"],link,post["created_time"],post["updated_time"])
	
	if author[1] != 'Hurricane Sandy Lost and Found Pets':
		cur.execute('insert into user_activities values(?,?,?,?)',[num_useractivities,author[0], author[1],\
			'post',getTime(created_time)])
		print 'inserted into user-activities table [%s, %s, \'post\', %s]' %(author[0],\
			author[1],created_time)
		num_useractivities += 1 
		
		cur.execute('insert into user_posts values(?,?,?)',[author[0], author[1], postid])
		print 'inserted into user-posts table [%s,%s,%s]' %(author[0], author[1], postid)

	if "likes" in post.keys():
		#if we do not have all the likes for the post then we need to get those likes 
		if post["likes"]["count"] != len(post["likes"]["data"]):
			likes_list = getAll(postid,"likes")
		else:
			likes_list = post["likes"]["data"]
		if deleted_flag == True:
			likes_list = post["likes"]["data"]
		# print 'DEBUGGING: iterating over likes: '+str(likes_list)
		for like in likes_list:
			cur.execute('insert into user_likes values(?,?,?,?)',[num_userlikes,like["id"],like["name"],postid])
			print 'inserted into the user-likes table [%s, %s, %s]' %(like["id"],like["name"],postid)
			num_userlikes += 1

			cur.execute('insert into user_activities values(?,?,?,?,?)',[num_useractivities,like["id"],\
				like["name"], 'like',None])
			print 'inserted into user-activities table [%s, %s, \'like\']' %(like["id"],\
				like["name"])
			num_useractivities += 1 

	if "data" in post["comments"].keys():
		comments_list = post["comments"]["data"]
	else:
		comments_list = []
	#if we do not have all the comments for the post then we need to get those comments
	
	if post["comments"]["count"] != len(comments_list) and not deleted_flag:
			comments = getAll(postid, "comments")
			if comments_list is None:
				comments_list = post["comments"]["data"]
	
	if comments_list != None:
		for comment in comments_list:
			cur.execute('insert into user_comments values(?,?,?,?,?,?)',[comment["id"],\
				comment["from"]["id"],comment["from"]["name"],comment["message"],postid,\
				getTime(comment["created_time"])])
			print 'insert into the user-comments table [%s, %s, %s, %s,%s,%s]' %(comment["id"],comment["from"]["id"]\
				,comment["from"]["name"],comment["message"],postid,comment["created_time"])
			
			cur.execute('insert into user_activities values(?,?,?,?,?)',[num_useractivities,author[0], \
				author[1],'comment', getTime(comment["created_time"])])
			print 'insert into user-activities table [%s,%s,"comment",%s]' %(comment["from"]["id"]\
				,comment["from"]["name"],comment["created_time"])
			num_useractivities += 1
	cur.execute('insert into objects_list values(?,?)',[postid, 'n']) #%(postid,('y' if deleted_flag else 'n'))
 	print 'inserted into objects_list table (%s,%s)' %(postid,'n')		
	con.commit()

	print 'exiting function'


def checkIfDeleted(postid):
	
	global ACCESS_TOKEN	

	graph = GraphAPI(ACCESS_TOKEN)
	try:
		post = graph.get(postid)
	except:
		return True
	return False

def getAccessToken():
	ACCESS_TOKEN = raw_input("Enter a valid Facebook access token: ")

def createTables():
	con = None
	try:
		con = sql.connect('sandyspets')
		cur = con.cursor()
		cur.execute("CREATE TABLE objects_list (objectid TEXT, deleted TEXT)")#key objectid
		cur.execute("CREATE TABLE user_comments (commentid TEXT, userid TEXT, userName TEXT, comment TEXT,post_id TEXT, created_time timestamp )") #primary key commentid
		cur.execute("CREATE TABLE user_likes(userlikesid, userid TEXT, userName TEXT, post_id TEXT)") #add ID
		cur.execute("CREATE TABLE post_info(postid TEXT, post TEXT, author_id TEXT, author_name share_count INT, like_count INT, post_type TEXT, link TEXT, created_time timestamp,updated_time timestamp)")
		cur.execute("CREATE TABLE user_activities(useractivitiesid INT, userid TEXT, userName TEXT, activity TEXT, timestamp timedate)") #addID
		cur.execute("CREATE TABLE user_posts(userpostsid INT, userid TEXT, userName TEXT, post_id TEXT)")	#add ID
		print 'tables created successfully!'

	except sql.Error, e:
	    print "Error %s:" % e.args[0]
	    sys.exit(1)
	    
	finally: 
	    if con:
	        con.close()

def cleandb():
	con = None
	try:
		con = sql.connect('sandyspets')
		cur = con.cursor()
		cur.execute("delete from objects_list;")#key objectid
		cur.execute("delete from user_comments;") #primary key commentid
		cur.execute("delete from user_likes;") #add ID
		cur.execute("delete from post_info;")
		cur.execute("delete from user_activities;") #addID
		cur.execute("delete from user_posts;")	#add ID
		con.commit()
		print 'tables cleaned successfully!'

	except sql.Error, e:
	    print "Error %s:" % e.args[0]
	    sys.exit(1)
	    
	finally: 
	    if con:
	        con.close()
def getTime(timestampstring):
	timeval= time.strptime(timestampstring[:-5],'%Y-%m-%dT%H:%M:%S')
	gmt_offset_seconds = int(timestampstring[-4:])*60*60
	return datetime.fromtimestamp(time.mktime(time.localtime(time.mktime(timeval)-gmt_offset_seconds)))

def getAll(objectid,attr):

	global ACCESS_TOKEN

	attr_list = []
	graph = GraphAPI(ACCESS_TOKEN)
	try:
		post = graph.get(objectid)
	except:
		print 'objectid deleted: ', objectid
		return None
	attr_vals = graph.get(objectid+'/'+attr)
	attr_list= attr_vals['data'] #list of [id name pairs]
	next_page  = attr_vals['paging']['next']
	expected_count = post[attr]['count']
	
	while (len(attr_list) < expected_count) and (next_page != ""):
		res = requests.get(next_page)#edit next_page
		if res.status_code ==200:
			attr_list = attr_list + res.json()['data']
			if 'next' in res.json()['paging'].keys():
				next_page = res.json()['paging']['next']
				# print "[DEBUGGING]  next page: "+str(next_page)
				# print "[DEBUGGING]  attr_list: %s, expected_count: %s ",str(len(attr_list)),str(expected_count)
			else:
				next_page = ""
	return attr_list

#For Every file

# post_authors = []
# comment_authors = []
# active_users = []
#deleted_info = "deleted_posts.txt"

file_count = 3
deleted_counter = 0

#deleting all data from all tables
#cleandb()
con = None
getNewPosts()
try:
	con = sql.connect('sandyspets')
	cur = con.cursor()
	print 'cursor ...'
	#uncomment below lines
	# while file_count <= 3: 
	# 	sandy_file = open('SandysPets'+str(file_count)+'.txt','r')
	# 	#out_file = open('tmp','w')
	# 	# deleted_posts_file = open(deleted_info,'w')
	# 	print 'reading from SandysPets2.txt...'

	# 	file_count += file_count
	# 	for line in sandy_file:
	# 		json_line = simplejson.loads(line)
	# 		#print line
	# 		#out_file.write(line)
	# 		deleted_flag = False
	# 		for element in json_line["data"]:
	# 			postid = element["id"]
	# 			deleted_flag= checkIfDeleted(postid)
	# 			if deleted_flag == True:
	# 				deleted_counter += 1
	# 			insertPostinDB(element,deleted_flag)
	# 			cur.execute('insert into objects_list values(?,?)',[postid, ('y' if deleted_flag else 'n')]) #%(postid,('y' if deleted_flag else 'n'))
	# 			print 'inserted into deleted_objects table (%s,%s)' %(postid,('y' if deleted_flag else 'n'))
	# 			con.commit()

except sql.Error, e:
    print "Error %s:" % e.args[0]
    sys.exit(1)
finally: 
    if con:
        con.close()	
print 'num deleted objects: '+str(deleted_counter) + 'out of '+str(num_posts)+' posts'

				# num_posts  += 1
				# if "from" in element.keys():
				# 	author = [element["from"]["id"],element["from"]["name"]]
				# 	if author not in post_authors:
				# 		post_authors.append(author)
				
				# if "message" in element.keys():
				# 	message = element["message"]
				# else:
				# 	message = ""
				
				# if "shares" in element.keys():
				# 	shares_count = element["shares"]["count"]
				# else:
				# 	shares_count = 0
				
				# if "likes" in element.keys():
				# 	likes_count = element["likes"]["count"]
				# else:
				# 	likes_count = 0
				
				# if 'link' in element.keys():
				# 	link = element["link"]
				# else:
				# 	link = ""

				# cur.execute('insert into post_info values(?,?,?,?,?,?,?,?,?,?)',[postid, message, author[0],\
				# 	author[1],shares_count, likes_count,element["type"],link,getTime(element["created_time"]),getTime(element["updated_time"])])
				# print 'inserted into post_info table [ %s, %s, %s, %s, %d, %d,%s,%s,%s,%s]' %(postid, message, author[0],\
				# 	author[1],shares_count, likes_count,element["type"],link,element["created_time"],element["updated_time"])
				
				# if author[1] != 'Hurricane Sandy Lost and Found Pets':
				# 	'''ERROR IN CONDITION!!!!!!!!!'''
					
				# 	cur.execute('insert into user_activities values(?,?,?,?)',[num_useractivities,author[0], author[1],'post'\
				# 		,getTime(created_time)])
				# 	print 'inserted into user-activities table [%s, %s, \'post\', %s]' %(author[0],\
				# 		author[1],created_time)
				# 	num_useractivities += 1 
					
				# 	cur.execute('insert into user_posts values(?,?,?)',[author[0], author[1], postid])
				# 	print 'inserted into user-posts table [%s,%s,%s]' %(author[0], author[1],\
				# 		postid)

				# if "likes" in element.keys():
				# 	if element["likes"]["count"] != len(element["likes"]["data"]):
				# 		likes_list = getAll(postid,"likes")
				# 	else:
				# 		likes_list = element["likes"]["data"]
				# 	if likes_list is None:
				# 		deleted_flag = True
				# 		deleted_counter = deleted_counter + 1
				# 		likes_list = element["likes"]["data"]
				# 	else:
				# 		for like in likes_list:
				# 			cur.execute('insert into user_likes values(?,?,?,?)',[num_userlikes,like["id"],like["name"],postid])
				# 			print 'inserted into the user-likes table [%s, %s, %s]' %(like["id"],like["name"],postid)
				# 			num_userlikes += 1

				# 			cur.execute('insert into user_activities values(?,?,?,?,?)',[num_useractivities,like["id"],like["name"],\
				# 		'like',None])
				# 			print 'inserted into user-activities table [%s, %s, \'post\']' %(like["id"],\
				# 		like["name"])
				# 	num_useractivities += 1 

				# if "data" in element["comments"].keys():
				# 	comments_list = element["comments"]["data"]
				# else:
				# 	comments_list = []
				
				# if element["comments"]["count"] != len(comments_list) and not deleted_flag:
				# 		comments = getAll(postid, "comments")
				# 		if comments_list is None:
				# 			comments_list = element["comments"]["data"]
				
				# if comments_list != None:
				# 	for comment in comments_list:
				# 		cur.execute('insert into user_comments values(?,?,?,?,?,?)',[comment["id"],\
				# 			comment["from"]["id"],comment["from"]["name"],comment["message"],postid,\
				# 			getTime(comment["created_time"])])
				# 		print 'insert into the user-comments table [%s, %s, %s, %s,%s,%s]' %(comment["id"],comment["from"]["id"]\
				# 			,comment["from"]["name"],comment["message"],postid,comment["created_time"])
						
				# 		cur.execute('insert into user_activities values(?,?,?,?,?)',[num_useractivities,author[0], author[1],'comment'\
				# 		,getTime(comment["created_time"])])
				# 		print 'insert into user-activities table [%s,%s,"comment",%s]' %(comment["from"]["id"]\
				# 			,comment["from"]["name"],comment["created_time"])
				# 		num_useractivities += 1

				# 		author = [comment["from"]["id"],comment["from"]["name"]]
				# 		if author not in comment_authors:
				# 			comment_authors.append(author)
				
				
#print(str(post_authors))
# print(str(comment_authors))
#insert post_authors and comment authors into a single table
#skip file_count=9