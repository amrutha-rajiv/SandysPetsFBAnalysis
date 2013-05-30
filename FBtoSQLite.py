#python libraries
import sys, time
from datetime import datetime
#third party libraries
import sqlite3 as sql
import requests, simplejson
from facepy import GraphAPI,get_application_access_token

#GLOBAL VARIABLES
#ACCESS_TOKEN =raw_input("Enter a valid Facebook access token: ")
ACCESS_TOKEN = get_application_access_token('168352736667694','1438d29eb39a5c6fd190ca8c9bdef97f')
page_name = raw_input("Enter the Facebook page name :")
DB_NAME = raw_input("Enter SQLite DB name: ")
num_userlikes = 1
num_posts = 0
num_albumlikes = 0

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

	likes_list = getAllInstancesOf(photo_id,"likes",type="photo")

	likes_count = len(likes_list)


	cur.execute('INSERT INTO photos_info values(?,?,?,?,?,?,?,?,?,?,?,?,?)',[photo_id, album_id, album_name , photo_desc, created_time , updated_time, author[1], author[0], picture_link, source_link, link,likes_count,shares_count])
	print 'INSERT INTO photos_info(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)' %(photo_id, album_id, album_name , photo_desc, created_time , updated_time, author[1], author[0], picture_link, source_link, link,str(likes_count),str(shares_count))

	if likes_count >0 :
		for like in likes_list:
			cur.execute('insert into user_likes_photos values(?,?,?,?)',[num_userlikes,like["id"],like["name"],photo_id])
			print 'inserted into the user-likes-photos table [%s, %s, %s]' %(like["id"],like["name"],photo_id)
			num_userlikes += 1	

	if "comments" in photo.keys():
		comments_list = getAllInstancesOf(photo_id,"comments",type="photo")
		if comments_list != None:
			for comment in comments_list:
				cur.execute('insert into user_comments_photos values(?,?,?,?,?,?,?)',[comment["id"],\
					comment["message"],comment["from"]["id"],comment["from"]["name"], comment["like_count"],\
					getTime(comment["created_time"]),photo_id])
				print 'insert into the user_comments_photos table [%s, %s, %s, %s,%s,%s,%s]' %(comment["id"],\
					comment["message"],comment["from"]["id"],comment["from"]["name"], str(comment["like_count"]),\
					photo_id, getTime(comment["created_time"]))				
	con.commit()

def insertAlbumInfo(album_id,DB_NAME):
	global ACCESS_TOKEN	

	graph = GraphAPI(ACCESS_TOKEN)
	album = graph.get(album_id)
	if "name" in album:
		album_name = album["name"]
	else:
		name = ""
	if "from" in album:
		author_id = album["from"]["id"]
		author_name = album["from"]["name"]
	else:
		author_id = ""
		author_name = ""

	if "description" in album:
		album_desc = album["description"]
	else:
		album_desc = ""
	if "link" in album:
		link = album["link"]
	else:
		link = ""

	if "cover_photo" in album:
		cover_photo_id = album["cover_photo"]
	else:
		cover_photo_id = ""

	if "count" in album:
		photo_count = album["count"]
	else:
		photo_count = 0

	if "type" in album:
		album_type = album["type"]
	else:
		album_type = ""

	if "created_time" in album:
		created_time = getTime(album["created_time"])
	else:
		created_time = ""
	if "updated_time" in album:
		updated_time = album["updated_time"]
	if "likes" in album:
		likes = getAllInstancesOf(album_id,"likes",type="album")
	else:
		likes = []
	if "comments" in album:
		comments = getAllInstancesOf(album_id,"comments",type="album")
	else:
		comments = []

	like_count = len(likes)
	comment_count = len(comments)

	(cur,con) = getDBConnection(DB_NAME)

	#insert album info into album_info
	cur.execute('insert into albums_info values(?,?,?,?,?,?,?,?,?,?,?,?,?)',[album_id,album_name,album_desc,author_id, author_name,link,cover_photo_id,photo_count,album_type,created_time,updated_time,like_count,comment_count])
	print "insert into albums_info values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" %(album_id,album_name,album_desc,str(author_id), author_name,link,str(cover_photo_id),str(photo_count),album_type,created_time,updated_time,str(like_count),str(comment_count))

	global num_albumlikes

	if like_count >0 :
		for like in likes:
			cur.execute('insert into albums_likes values(?,?,?,?)',[num_albumlikes,like["id"],like["name"],album_id])
			print 'inserted into the albums_likes table [%d, %s, %s, %s]' %(num_albumlikes,like["id"],like["name"],album_id)
			num_albumlikes += 1

	if comment_count>0:
		for comment in comments:
			cur.execute('insert into albums_comments values(?,?,?,?,?,?,?)',[comment["id"],\
				comment["message"],comment["from"]["id"],comment["from"]["name"], comment["like_count"],\
				album_id, getTime(comment["created_time"])])
			print 'insert into the albums_comments table [%s, %s, %s, %s,%s,%s,%s]' %(comment["id"],\
				comment["message"],comment["from"]["id"],comment["from"]["name"], comment["like_count"],\
				album_id, getTime(comment["created_time"]))	

	con.commit()
	con.close()


def insertAlbumsAndPhotosInDB(page_name,DB_NAME):
	global ACCESS_TOKEN	

	graph = GraphAPI(ACCESS_TOKEN)
	albums = graph.get(page_name+"/albums")
	if 'data' in albums.keys():
		albums = albums["data"]
	else: 
		return []
	existing_photos = getObjectsList(DB_NAME,col="photo_id",table="photos_info")
	existing_albums = getObjectsList(DB_NAME,col="album_id",table="albums_info")
	for element in albums:
		album_id = element['id']
		album_name = element['name']
		# if "Timeline Photos" in album_name:
		# 	continue
		if album_id not in existing_albums:
			insertAlbumInfo(album_id,DB_NAME)
		photos = getPhotoIds(album_id)
		(cur,con) = getDBConnection(DB_NAME)
		for photo in photos:
			if photo not in existing_photos:
				insertPhotoFromAlbum(DB_NAME, album_id,album_name,photo)

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

def getObjectsList(DB_NAME,col,table):
	
	con = None
	postid_list = []

	try:
		(cur,con) = getDBConnection(DB_NAME)
		sql_string = "SELECT DISTINCT "+col+" from "+table+";"
		cur.execute(sql_string)
		objectid_list = [tuple[0] for tuple in cur]
	except sql.Error, e:
	    print "Error %s:" % e.args[0]
	    sys.exit(1)
	    
	finally: 
	    if con:
	        con.close()
	return objectid_list

def getPageFeed(page_name,DB_NAME):
	global ACCESS_TOKEN	

	graph = GraphAPI(ACCESS_TOKEN)
	response = graph.get(page_name+'/?fields=feed.fields(from)')
	next_page  = response['feed']['paging']['next']
	response = response['feed']	
	allposts = getObjectsList(DB_NAME,"postid","post_info")
	while next_page!= "":	
		for element in response['data']:
			postid = element['id']
	  		if postid not in allposts:
	  			post_authorid = element['from']['id']
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

#this is to be deprecated\
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
	
	global num_userlikes
	global num_posts
	global num_userlikes_userpost

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
		
	if "likes" in post.keys():
		likes_list = getAllInstancesOf(postid,"likes")
		for like in likes_list:
			cur.execute('insert into user_likes values(?,?,?,?)',[num_userlikes,like["id"],like["name"],postid])
			print 'inserted into the user_likes table [%s, %s, %s]' %(like["id"],like["name"],postid)
			num_userlikes += 1

	if "comments" in post.keys():
		comments_list = getAllInstancesOf(postid, "comments")

		if comments_list != None:
			for comment in comments_list:
				cur.execute('insert into user_comments values(?,?,?,?,?,?)',[comment["id"],\
					comment["from"]["id"],comment["from"]["name"],comment["message"],postid,\
					getTime(comment["created_time"])])
				print 'insert into the user_comments table [%s, %s, %s, %s,%s,%s]' %(comment["id"],comment["from"]["id"]\
					,comment["from"]["name"],comment["message"],postid,comment["created_time"])
				
	con.commit()

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

		cur.execute('CREATE TABLE IF NOT EXISTS photos_info(photo_id TEXT, album_id TEXT, album_name TEXT, photo_desc TEXT, created_time timestamp, updated_time timestamp, author_name TEXT, author_id TEXT, picture_link TEXT, source_link TEXT, link TEXT,likes_count INT,shares_count INT );')
		cur.execute('CREATE TABLE IF NOT EXISTS user_comments_photos(commentid TEXT, comment TEXT, userid TEXT, userName TEXT, likes_count INT, created_time timestamp, photo_id TEXT);')	
		cur.execute('CREATE TABLE IF NOT EXISTS user_likes_photos(userlikesid INT, userid TEXT, userName TEXT, photo_id TEXT);')
		cur.execute("CREATE TABLE IF NOT EXISTS user_comments (commentid TEXT, userid TEXT, userName TEXT, comment TEXT,post_id TEXT, created_time timestamp )") #primary key commentid
		cur.execute("CREATE TABLE IF NOT EXISTS user_likes(userlikesid, userid TEXT, userName TEXT, post_id TEXT)") #add ID
		cur.execute("CREATE TABLE IF NOT EXISTS post_info(postid TEXT, post TEXT, author_id TEXT, author_name share_count INT, like_count INT, post_type TEXT, link TEXT, created_time timestamp,updated_time timestamp)")
		cur.execute("CREATE TABLE IF NOT EXISTS albums_info(album_id TEXT,album_name TEXT, album_desc TEXT, author_id TEXT, author_name TEXT,link TEXT,cover_photo_id TEXT,photo_count INT, album_type TEXT,created_time TIMESTAMP, updated_time TIMESTAMP, like_count INT, comment_count INT);")
		cur.execute("CREATE TABLE IF NOT EXISTS albums_likes(num_albumlikes INT,userid TEXT,username TEXT, album_id TEXT);")		
		cur.execute("CREATE TABLE IF NOT EXISTS albums_comments(commentid TEXT, comment TEXT, author_id TEXT, author_name TEXT, like_count INT, album_id TEXT, created_time TIMESTAMP);")

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
		cur.execute("delete from user_comments;") #primary key commentid
		cur.execute("delete from user_likes;") #add ID
		cur.execute("delete from post_info;")
		cur.execute("delete from user_comments_photos;")
		cur.execute("delete from user_likes_photos;") #add ID
		cur.execute("delete from photos_info;")
		cur.execute("delete from albums_likes") #add ID
		cur.execute("delete from albums_info;")
		cur.execute("delete from albums_comments;")
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

def getAllInstancesOf(objectid,attr,type="post"):

	global ACCESS_TOKEN

	attr_list = []
	graph = GraphAPI(ACCESS_TOKEN)
	try:
		post = graph.get(objectid)
	except:
		print 'objectid deleted: ', objectid
		return None
	if type == "post":
		attr_vals = graph.get(objectid+'/'+attr+'?summary=true')
	else:
		attr_vals = graph.get(objectid+'/'+attr)
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

#create all tables if tables don't exist in DB
createTables(DB_NAME)
# print "page_name: "+page_name
#insert all photos from sandyspets page to the sqlitedb
insertAlbumsAndPhotosInDB(page_name, DB_NAME)
#pull Timeline Data
getPageFeed(page_name,DB_NAME)