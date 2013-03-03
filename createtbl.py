import sqlite3 as sql
import sys

con = None
try:
	con = sql.connect('sandyspets')
	cur = con.cursor()
	#cur.execute("CREATE TABLE objects_list (objectid TEXT, deleted TEXT)")#key objectid
	#cur.execute("CREATE TABLE user_comments (commentid TEXT, userid TEXT, userName TEXT, comment TEXT,post_id TEXT, created_time timestamp )") #primary key commentid
	#(comment["id"],comment["from"]["id"],comment["from"]["name"],comment["message"],postid,comment["created_time"])
	#cur.execute("CREATE TABLE user_likes(userlikesid, userid TEXT, userName TEXT, post_id TEXT)") #add ID
	cur.execute("CREATE TABLE post_info(postinfoid INT, postid TEXT, post TEXT, author_id TEXT, author_name share_count INT, like_count INT, post_type TEXT, link TEXT, created_time timestamp,updated_time timestamp)")
	#(postid, message, author[0],author[1],shares_count, likes_count,element["type"],link,element["created_time"],element["updated_time"])
	cur.execute("CREATE TABLE user_activities(useractivitiesid INT, userid TEXT, userName TEXT, activity TEXT, timestamp timedate)") #addID
	cur.execute("CREATE TABLE user_posts(userpostsid INT, userid TEXT, userName TEXT, post_id TEXT)")	#add ID
	print 'tables created successfully!'

except sql.Error, e:
    print "Error %s:" % e.args[0]
    sys.exit(1)
    
finally: 
    if con:
        con.close()
