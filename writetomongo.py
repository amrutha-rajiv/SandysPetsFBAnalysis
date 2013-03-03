# from pymongo import mongoclient

# connection = MongoClient()
# db = connection['facebook']
# collection = db['sandyspets']

import simplejson

file_count = 2
post_authors = []
comment_authors = []
active_users = []
while file_count <= 2:
	sandy_file = open('SandysPets'+str(file_count)+'.txt','r')
	out_file = open('tmp','w')
	
	for line in sandy_file:
		json_line = simplejson.loads(line)
		#print line
		#out_file.write(line)
		for element in json_line["data"]:
			if "from" in element.keys():
				author = [element["from"]["id"],element["from"]["name"]]
				if author not in post_authors:
					post_authors.append(author)
			if "comments" in element.keys():
				if "data" in element["comments"].keys():
					for ele in element["comments"]["data"]:
						if "from" in ele.keys():
							author = [ele["from"]["id"],ele["from"]["name"]]
							if author not in comment_authors:
								comment_authors.append(author)

	
	file_count = file_count+1
print(str(post_authors))
print(str(comment_authors))
#skip file_count=9
#alasandhala pulusu
#next step: figure out how to get  to the user and user groups.t