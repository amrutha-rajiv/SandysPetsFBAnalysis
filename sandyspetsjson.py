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


def getNewPostsJSON():
	global ACCESS_TOKEN

	graph = GraphAPI(ACCESS_TOKEN)
	response = graph.get('sandyspets/?fields=posts.fields(id)')
	next_page  = response['posts']['paging']['next']
	response = response['posts']
	# out_file = open('test1.txt','w')	
	while next_page!= "":	
		for element in response['data']:
	  		print getPostbyId(element['id'])
	  	response = requests.get(next_page)
		if response.status_code == 200:
			response = response.json()
			try:
				#if 'next' in response.json()['paging'].keys():
				next_page = response['paging']['next']
			except:
				next_page = ""

def getPostbyId(postid):
	global ACCESS_TOKEN

	graph = GraphAPI(ACCESS_TOKEN)
	post = graph.get(postid)

	global num_userposts
	global num_useractivities
	global num_userlikes
	global num_posts

	postid = post["id"]
	num_posts  += 1

	if "likes" in post.keys():
		#if we do not have all the likes for the post then we need to get those likes 
		if post["likes"]["count"] != len(post["likes"]["data"]):
			likes_list = getAll(postid,"likes")
		else:
			likes_list = post["likes"]["data"]
		# print 'DEBUGGING: iterating over likes: '+str(likes_list)
		post["likes"]["data"] = likes_list

	if "data" in post["comments"].keys():
		comments_list = post["comments"]["data"]
	else:
		comments_list = []
	#if we do not have all the comments for the post then we need to get those comments
	
	if post["comments"]["count"] != len(comments_list):
			comments_list = getAll(postid, "comments")
			if comments_list is None:
				comments_list = post["comments"]["data"]
	post["comments"]["data"] = comments_list

	return post

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

getNewPostsJSON()