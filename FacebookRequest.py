import facebook
#from facepy import GraphAPI
import requests, simplejson

oauth_access_token = 'AAACEdEose0cBACYEkm3Wc6d0AgeAEXsL7lhEXZADbKNbBJ0hTldBaWtCTVyioZAQ6Pf9sEV1FHwGG4AmHaK36EPpJ7n0wi7rSPiMSHuQZDZD'
graph = facebook.GraphAPI(oauth_access_token)
file_no = 15
output_file = open('SandysPets'+str(file_no)+'.txt','w')
page = ''
res = requests.get('https://graph.facebook.com/403922739676650/posts?limit=25&access_token=AAACEdEose0cBACYEkm3Wc6d0AgeAEXsL7lhEXZADbKNbBJ0hTldBaWtCTVyioZAQ6Pf9sEV1FHwGG4AmHaK36EPpJ7n0wi7rSPiMSHuQZDZD&until=1351478115')
page_count = 114
while (str(page)!= res.text) and (res.status_code == 200):
	page = res.text
	page_count = page_count + 1
	print 'copying page: '+str(page_count)
	if page_count % 10 == 0:
		file_no = file_no + 1
		output_file = open('SandysPets'+str(file_no)+'.txt','w')
	output_file.write(str(page))
	res_msg = simplejson.loads(page)
	res = requests.get(res_msg['paging']['next'])
	
if res.status_code != 200:
	print str(res.text)

print 'number of pages: '+str(page_count)

# graph = facebook.GraphAPI(oauth_access_token)
## page = graph.get_object('me')
## print str(page)
# page = graph.get_object('SandysPets/posts')#?fields=posts
# output_file = open('SandysPets2.txt','w')
# output_file.write(str(page))
# print str(page['paging']['next'])
#requests.get('https://graph.facebook.com/SandysPets/posts?access_token=AAAEe3QwI0a8BAOvHiZC46HpIa7kZBY0hXiND9yxsPJmrJQIZC2V1EPxZBHcvwZAZBkjZBZBv54VTwE8OWhGXopx4EqcRRLm6BPZCj6ZAXMpDjVBQZDZD')


##status: unable to get info for the next page. :| 

# since = 1351404000
# until = (since + 3600)
# incr = 0 

# for incr in range(86400000):
# 	page = graph.get('SandysPets/posts?since='+str(since)+'&until='+str(until))
# 	output_file.write(str(page))
# 	incr = incr + 3600000
# 	since = until
# 	until = (until +3600000)/10000

# res = requests.get('https://graph.facebook.com/403922739676650/posts?access_token=AAACEdEose0cBAJ4bK68uHGWSrrOmIerYZB2dr8JXz1sDCYNcn40ovXi3IYqT96cHrtx3sZCbYk6TD2PrLSjiYUEaOoSDvxZATGFfEH7rwZDZD&limit=25&until=1351552576')
#res.text

#page = graph.get_object('SandysPets/posts')
#output_file.write(str(page))
#res = requests.get(page['paging']['next'])
