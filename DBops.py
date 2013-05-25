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

def getDBConnection(DB_NAME):
	try:
		con = sql.connect(DB_NAME)
		cur = con.cursor()	
		return cur,con
	except sql.Error, e:
		print "Error %s:" % e.args[0]
		sys.exit(1)

def insertPhotoFromAlbum(DB_NAME,album_id,album_name,photo_id):
	global ACCESS_TOKEN	
	global num_userlikes

	photo_desc = ""	
	source_link=""
	picture_link =""
	link =""
	likes_count = 0
	shares_count =0

	graph = GraphAPI(ACCESS_TOKEN)	
	photo = graph.get(photo_id)

	(cur,con) = getDBConnection(DB_NAME)
	if "from" in photo.keys():
		author = [photo["from"]["id"],photo["from"]["name"]]
	
	if "name" in photo.keys():
		photo_desc = photo["name"]

	if "picture" in photo.keys():
		picture_link = photo["picture"]
			
	if "source" in photo.keys():
		source_link = photo["source"]

	if "link" in photo.keys():
		link = photo["link"]

	created_time = getTime(photo["created_time"])
	updated_time = getTime(photo["updated_time"])

	likes_list = getAll(photo_id,"likes")

	likes_count = len(likes_list)


	cur.execute('INSERT INTO photos_info values(?,?,?,?,?,?,?,?,?,?,?,?,?)',[photo_id, album_id, album_name , photo_desc, created_time , updated_time, author[1], author[0], picture_link, source_link, link,likes_count,shares_count])
	print 'INSERT INTO photos_info(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)' %(photo_id, album_id, album_name , photo_desc, created_time , updated_time, author[1], author[0], picture_link, source_link, link,str(likes_count),str(shares_count))

	if likes_count >0 :
		for like in likes_list:
			cur.execute('insert into user_likes_photos values(?,?,?,?)',[num_userlikes,like["id"],like["name"],photo_id])
			print 'inserted into the user-likes-photos table [%s, %s, %s]' %(like["id"],like["name"],photo_id)
			num_userlikes += 1	

	if "comments" in photo.keys():
		comments_list = getAll(photo_id,"comments")
		if comments_list != None:
			for comment in comments_list:
				cur.execute('insert into user_comments_photos values(?,?,?,?,?,?,?)',[comment["id"],\
					comment["message"],comment["from"]["id"],comment["from"]["name"], comment["like_count"],\
					photo_id, getTime(comment["created_time"])])
				print 'insert into the user_comments_photos table [%s, %s, %s, %s,%s,%s,%s]' %(comment["id"],\
					comment["message"],comment["from"]["id"],comment["from"]["name"], str(comment["like_count"]),\
					photo_id, getTime(comment["created_time"]))				
	con.commit()

def insertAllPhotosInDB(page_name,DB_NAME):
	global ACCESS_TOKEN	

	graph = GraphAPI(ACCESS_TOKEN)
	album_ids = []
	albums = graph.get(page_name+"/albums")
	if 'data' in albums.keys():
		albums = albums["data"]
	else: 
		return []
	existing_photos = getObjectsList(DB_NAME,col="photo_id",table="photos_info")
	for element in albums:
		album_id = element['id']
		album_name = element['name']
		# if "Timeline Photos" in album_name:
		# 	continue
		album_ids.append(album_id)
		photos = getPhotoIds(album_id)
		(cur,con) = getDBConnection(DB_NAME)
		for photo in photos:
			if photo not in existing_photos:
				insertPhotoFromAlbum(DB_NAME, album_id,album_name,photo)
			#uncomment below to insert ONLY photo id into the db
		# 	try:
		# 		cur.execute('INSERT INTO photos_albums values(?,?,?)',[photo,album_id,album_name])
		# 	except sql.Error, e:
		# 		print "Error %s:" % e.args[0]
		# 		sys.exit(1)
		# con.commit()

	return album_ids

#TODO: Test and Execute!!!!
#get ALL the photos
def getPhotoIds(album_id):
	global ACCESS_TOKEN	

	graph = GraphAPI(ACCESS_TOKEN)
	photos_list = []
	photos = graph.get(album_id+"/photos")
	photos_list= list(element['id'] for element in photos['data'] )
	if 'next' in photos['paging'].keys():
		next_page  = photos['paging']['next']
	else:
		next_page = ""
	while (next_page != ""):
		res = requests.get(next_page)#edit next_page
		if res.status_code ==200:
			photos_list = photos_list +list( element['id'] for element in res.json()['data'])
			try:
				if 'next' in res.json()['paging'].keys():
					next_page = res.json()['paging']['next']
				else:
					next_page = ""
			except:
				if 'paging' not in res.json().keys():
					print res.json()
				next_page = ""
	
	return photos_list

def getObjectsList(DB_NAME,col="objectid",table="objects_list"):
	
	con = None
	postid_list = []

	try:
		(cur,con) = getDBConnection(DB_NAME)
		sql_string = "SELECT "+col+" from "+table+";"
		cur.execute(sql_string)
		postid_list = [tuple[0] for tuple in cur]
	except sql.Error, e:
	    print "Error %s:" % e.args[0]
	    sys.exit(1)
	    
	finally: 
	    if con:
	        con.close()
	return postid_list

def getPostsList(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
	results = cur.execute("select postid from post_info;")
	list_posts = [result[0] for result in results]
	return list_posts

def getNewPosts(PAGE_NAME,DB_NAME):
	
	global ACCESS_TOKEN

	graph = GraphAPI(ACCESS_TOKEN)

	allposts = getPostsList(DB_NAME)
	response = graph.get(PAGE_NAME+'/?fields=posts.fields(id)')
	next_page  = response['posts']['paging']['next']
	response = response['posts']	
	while next_page!= "":	
		for element in response['data']:
			postid = element['id']
	  		if postid not in allposts:
  				insertPostinDB(graph.get(postid),DB_NAME)
	  	response = requests.get(next_page)
		if response.status_code == 200:
			try:
				#if 'next' in response.json()['paging'].keys():
				next_page = response.json()['paging']['next']
			except:
				next_page = ""
				print 'response: '+str(response.json())
   			response = response.json()		
   			
def insertPostinDB(post,DB_NAME):
	
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
	else:
		author=["",""]
	
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

	(cur,con) = getDBConnection(DB_NAME)
	cur.execute('insert into post_info values(?,?,?,?,?,?,?,?,?,?)',[postid, message, author[0],\
		author[1],shares_count, likes_count,post["type"],link,getTime(post["created_time"]),\
		getTime(post["updated_time"])])
	print 'inserted into post_info table [ %s, %s, %s, %s, %d, %d,%s,%s,%s,%s]' %(postid, message, author[0],\
		author[1],shares_count, likes_count,post["type"],link,post["created_time"],post["updated_time"])
	
	# if author[1] != 'Hurricane Sandy Lost and Found Pets':
	# 	cur.execute('insert into user_activities values(?,?,?,?)',[num_useractivities,author[0], author[1],\
	# 		'post',getTime(created_time)])
	# 	print 'inserted into user-activities table [%s, %s, \'post\', %s]' %(author[0],\
	# 		author[1],created_time)
	# 	num_useractivities += 1 
		
		# cur.execute('insert into user_posts values(?,?,?)',[author[0], author[1], postid])
		# print 'inserted into user-posts table [%s,%s,%s]' %(author[0], author[1], postid)

	if "likes" in post.keys():
		likes_list = getAll(postid,"likes")
		for like in likes_list:
			cur.execute('insert into user_likes values(?,?,?,?)',[num_userlikes,like["id"],like["name"],postid])
			print 'inserted into the user-likes table [%s, %s, %s]' %(like["id"],like["name"],postid)
			num_userlikes += 1

			# cur.execute('insert into user_activities values(?,?,?,?,?)',[num_useractivities,like["id"],\
			# 	like["name"], 'like',None])
			# print 'inserted into user-activities table [%s, %s, \'like\']' %(like["id"],\
			# 	like["name"])
			# num_useractivities += 1 
	if "comments" in post.keys():
		comments_list = getAll(postid, "comments")
		if comments_list is None:
			comments_list = post["comments"]["data"]

		if comments_list != None:
			for comment in comments_list:
				cur.execute('insert into user_comments values(?,?,?,?,?,?)',[comment["id"],\
					comment["from"]["id"],comment["from"]["name"],comment["message"],postid,\
					getTime(comment["created_time"])])
				print 'insert into the user-comments table [%s, %s, %s, %s,%s,%s]' %(comment["id"],comment["from"]["id"]\
					,comment["from"]["name"],comment["message"],postid,comment["created_time"])
				
				# cur.execute('insert into user_activities values(?,?,?,?,?)',[num_useractivities,author[0], \
				# 	author[1],'comment', getTime(comment["created_time"])])
				# print 'insert into user-activities table [%s,%s,"comment",%s]' %(comment["from"]["id"]\
				# 	,comment["from"]["name"],comment["created_time"])
				# num_useractivities += 1
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

def createTables(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
	try:
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

def cleandb(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
	try:
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
	attr_vals = graph.get(objectid+'/'+attr+'?summary=true')
	attr_list= attr_vals['data'] #list of [id name pairs]
	next_flag = False
	if 'paging' in attr_vals.keys():
		if 'next' in attr_vals['paging'].keys():
			next_flag = True		
	if next_flag:
		next_page  = attr_vals['paging']['next']		
	else:
		next_page = ""
	#expected_count = post[attr]['count']
	
	while (next_page != ""):
		#print next_page
		res = requests.get(next_page)#edit next_page
		if res.status_code ==200:
			attr_list = attr_list + res.json()['data']
			try:
				if 'next' in res.json()['paging'].keys():
					next_page = res.json()['paging']['next']
					# print "[DEBUGGING]  next page: "+str(next_page)
					# print "[DEBUGGING]  attr_list: %s, expected_count: %s ",str(len(attr_list)),str(expected_count)
				else:
					next_page = ""
			except:
				if 'paging' not in res.json().keys():
					print res.json()
				next_page = ""
	return attr_list

def pullFacebookDataFromTextFile(DB_NAME):
	(cur,con) = getDBConnection(DB_NAME)
	try:		
		while file_count <= 3: 
			sandy_file = open('sandyspets6-8'+str(file_count)+'.txt','r')
			#out_file = open('tmp','w')
			# deleted_posts_file = open(deleted_info,'w')
			print 'reading from SandysPets2.txt...'

			file_count += file_count
			for line in sandy_file:
				json_line = simplejson.loads(line)
				#print line
				#out_file.write(line)
				deleted_flag = False
				for element in json_line["data"]:
					postid = element["id"]
					deleted_flag= checkIfDeleted(postid)
					if deleted_flag == True:
						deleted_counter += 1
					insertPostinDB(element,deleted_flag)
					cur.execute('insert into objects_list values(?,?)',[postid, ('y' if deleted_flag else 'n')]) #%(postid,('y' if deleted_flag else 'n'))
					print 'inserted into deleted_objects table (%s,%s)' %(postid,('y' if deleted_flag else 'n'))
					con.commit()
	except sql.Error, e:
	    print "Error %s:" % e.args[0]
	    sys.exit(1)
	finally: 
	    if con:
	        con.close()	
	print 'num deleted objects: '+str(deleted_counter) + 'out of '+str(num_posts)+' posts'

#insert all photos from sandyspets page to the sqlitedb
insertAllPhotosInDB('okpets','okpets524')
#create all tables in the new DB
# createTables('sandyspets524')
#pull Timeline Data
#getNewPosts('okpets','okpets524')