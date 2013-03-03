import sqlite3 as sql
import sys, time, requests, simplejson
from facepy import GraphAPI
from datetime import datetime

ACCESS_TOKEN = 'AAACEdEose0cBACpZCZCRXV2x0a6OJ4atZC37K3u3ZAnLQ7u32qkzN1laGOXZB4ZAvc1JZAIw6Y4e0qo5pfWFdZBHbLY8AtHTY8VUOVAsFNzZAegZDZD'
file_count = 3
post_authors = []
comment_authors = []
active_users = []
deleted_info = "deleted_posts.txt"

def getTime(timestampstring):
	timeval= time.strptime(timestampstring[:-5],'%Y-%m-%dT%H:%M:%S')
	gmt_offset_seconds = int(timestampstring[-4:])*60*60
	return datetime.fromtimestamp(time.mktime(time.localtime(time.mktime(timeval)-gmt_offset_seconds)))

def getAll(objectid,attr):
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
	
	while len(attr_list) < expected_count:
		res = requests.get(next_page)#edit next_page
		if res.status_code ==200:
			attr_list = attr_list + res.json()['data']
			next_page = res.json()['paging']['next']

#For Every file
deleted_counter = 0
num_posts = 0
con = None
try:
	con = sql.connect('sandyspets')
	cur = con.cursor()
	print 'cursor ...'
	while file_count <= 3: 
		sandy_file = open('SandysPets'+str(file_count)+'.txt','r')
		#out_file = open('tmp','w')
		deleted_posts_file = open(deleted_info,'w')
		print 'reading from SandysPets2.txt...'

		file_count = file_count + 1
		for line in sandy_file:
			json_line = simplejson.loads(line)
			#print line
			#out_file.write(line)
			deleted_flag = False
			for element in json_line["data"]:
				postid = element["id"]
				num_posts  = num_posts + 1
				if "from" in element.keys():
					author = [element["from"]["id"],element["from"]["name"]]
					if author not in post_authors:
						post_authors.append(author)
				
				if "message" in element.keys():
					message = element["message"]
				else:
					message = ""
				
				if "shares" in element.keys():
					shares_count = element["shares"]["count"]
				else:
					shares_count = 0
				
				if "likes" in element.keys():
					likes_count = element["likes"]["count"]
				else:
					likes_count = 0
				
				if 'link' in element.keys():
					link = element["link"]
				else:
					link = ""

				print 'insert into post_info table [ %s, %s, %s, %s, %d, %d,%s,%s,%s,%s]' %(postid, message, author[0],\
					author[1],shares_count, likes_count,element["type"],link,element["created_time"],element["updated_time"])
				cur.execute('insert into post_info values(?,?,?,?,?,?,?,?,?,?)',[postid, message, author[0],\
					author[1],shares_count, likes_count,element["type"],link,getTime(element["created_time"]),getTime(element["updated_time"])])
				#insert into post_info table [post-id, post-message, post-author-id, post-author-name,share-count, like count, post-type, link, created-time, updated-time]
				if author[1] != 'Hurricane Sandy Lost and Found Pets':
					'''ERROR IN CONDITION!!!!!!!!!'''
					print 'insert into user-activities table [%s, %s, \'post\', %s]' %(author[0],\
						author[1],created_time)
					cur.execute('insert into user_activities values(?,?,?)',[author[0], author[1],'post'\
						,getTime(created_time)])
					#insert into user-activities table [user-id, user-name, activity-name time-stamp] 
					print 'insert into user-posts table [%s,%s,%s]' %(author[0], author[1],\
						postid) 
					cur.execute('insert into user_posts values(?,?,?)',[author[0], author[1], postid])
					#insert into user-posts table [user-id, user-name, post-id]
				if "likes" in element.keys():
					if element["likes"]["count"] != len(element["likes"]["data"]):
						likes_list = getAll(postid,"likes")
					else:
						likes_list = element["likes"]["data"]
					if likes_list is None:
						deleted_flag = True
						deleted_counter = deleted_counter + 1
						likes_list = element["likes"]["data"]
					else:
						for like in likes_list:
							print 'insert into the user-likes table [%s, %s, %s]' %(like["id"],like["name"],postid)
							cur.execute('insert into user_likes values(?,?,?)',[like["id"],like["name"],postid])
							#insert into the user-likes table [user-id, user-name, post-id]
				if "data" in element["comments"].keys():
					comments_list = element["comments"]["data"]
				else:
					comments_list = []
				
				if element["comments"]["count"] != len(comments_list) and not deleted_flag:
						comments = getAll(postid, "comments")
						if comments_list is None:
							comments_list = element["comments"]["data"]
				
				if comments_list != None:
					for comment in comments_list:
						print 'insert into the user-comments table [%s, %s, %s, %s,%s,%s]' %(comment["id"],comment["from"]["id"]\
							,comment["from"]["name"],comment["message"],postid,comment["created_time"])
						cur.execute('insert into user_comments values(?,?,?,?,?,?)',[comment["id"],\
							comment["from"]["id"],comment["from"]["name"],comment["message"],postid,\
							getTime(comment["created_time"])])
						#insert into user-comments table [comment-id, user-id, user-name,comment-text,post-id, timestamp]
						print 'insert into user-activities table [%s,%s,"comment",%s]' %(comment["from"]["id"]\
							,comment["from"]["name"],comment["created_time"])
						cur.execute('insert into user_activities values(?,?,?,?)',[author[0], author[1],'comment'\
						,getTime(comment["created_time"])])
						#insert into user-activities table [user-id, user-name, activity-name,timestamp]				
						author = [comment["from"]["id"],comment["from"]["name"]]
						if author not in comment_authors:
							comment_authors.append(author)
				#deleted_posts_file = open(deleted_info,'a')
				#deleted_posts_file.write(str(postid)+'\n')
				#deleted_posts_file.close()	
				print 'insert into deleted_objects table (%s,%s)' %(postid,('y' if deleted_flag else 'n'))
				cur.execute('insert into objects_list values(?,?)',[postid, ('y' if deleted_flag else 'n')]) #%(postid,('y' if deleted_flag else 'n'))
				con.commit()
except sql.Error, e:
    print "Error %s:" % e.args[0]
    sys.exit(1)
finally: 
    if con:
        con.close()	

print(str(post_authors))
# print(str(comment_authors))
#insert post_authors and comment authors into a single table
print 'num deleted objects: '+str(deleted_counter) + 'out of '+str(num_posts)+' posts'

#skip file_count=9
#alasandhala pulusu
#next step: figure out how to get  to the user and user groups.t