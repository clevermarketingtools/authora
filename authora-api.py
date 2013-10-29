#!/usr/bin/python

# yay, authora has an API!
	# /search/content/topic
	# /search/author/name
	# /search/author/account
# bottle app w/ 3 routes for generating Authora Tables
	# queries are limited to >= 500 results returned
	# queries are sorted by totalShares
# it's async using gevent & greenlets
	#should look into that for re-tooling the workers later
# uses a mongo optimization found @ http://emptysquare.net/blog/requests-in-python-and-mongodb/
	# worked so well, it was back ported to worker-arc-crawler-final.py
# hit it with 15000 async http requests as a test and it was fine

try:
	import sys, logging, json, urllib

	import pymongo
	from pymongo import *

	from gevent import monkey; monkey.patch_all()
	from time import sleep
	from bottle import route, run, request
	
	import raven
	from raven import Client

	from raven.handlers.logging import SentryHandler
	from raven.conf import setup_logging
	
	logger = logging.getLogger()
	# If Sentry Fails, might as well fail the whole thing.
	client = Client('')
	handler = SentryHandler(client)
	setup_logging(handler)
except:
	print 'Fatal Error - Missing Dependency: ', sys.exc_info()[0]
	sys.exit()

try:
	conn = Connection('',27017,auto_start_request=False)
	db = conn.authora
	authors = db[u'authors']
	content = db[u'content'];
except Exception, ex:
	logging.exception('mongodb connection error')
	sys.exit()


@route('/search/content/topic')
def searchAuthoraByTopic():
	
	try:
		topic = request.query.get('topic')
	except Exception, ex:
		logging.exception('no name error: searchAuthoraByTopic()')
		topic = None
	try:
		email = request.query.get('email')
	except Exception, ex:
		logging.exception('no email error: searchAuthoraByTopic()')
		email = None

	if topic is None:
		yield '{"response":{"error":"Sorry, the server received a malformed request. Please refresh the page and try your search again."}}'
	else:
	

		try:
			topic = urllib.unquote(topic)
			topic = unicode(topic.strip(),errors='ignore')
			topic = topic.lower()
		except Exception, ex:
			logging.exception('name to unicode error: searchAuthoraByTopic()')
		
	
		resultList = content.find({u'topics':topic}).sort(u'totalShares', pymongo.DESCENDING).limit(100)
		resultCount = resultList.count()
		conn.end_request()
	
		if resultCount > 0:
			count = 0
			result = ''
			for record in resultList:
				authorID = record[u'authorID']
				authorName = record[u'authorName']
				url = record[u'url']
				urlNoHttp = url[7:]
				urlEncoded = urllib.quote_plus(url)
				urlNoHttp = urllib.quote_plus(urlNoHttp)
				shares = record[u'shares']
				result = result + authorID + ',' + authorName + ',' + url + ','
				result = result + 'http://www.linkdiagnosis.com/?q=' + urlEncoded + ',http://www.opensiteexplorer.org/links.html?no_redirect=1&page=1&site=' + url + ',http://www.majesticseo.com/reports/site-explorer?folder=&q=' + urlEncoded + '?IndexDataSource=H' 
				for item in shares:
					for key in item.keys():
						result = result + ',' + str(item[key])
				count = count + 1
				if count < resultCount:
					result = result + ';'
			result = result.rstrip(';')
			yield result
			
		else:
			if email is not None:		
				try:
					email = urllib.unquote(email)
					email = unicode(email.strip(),errors='ignore')
					email = email.lower()
				except Exception, ex:
					logging.exception('email to unicode error: searchAuthoraByTopic()')
				yield '{"response":{"message":"Sorry, Authora has no results for ' + topic + '. We will alert you by email when we have results for that request. Please check back soon!"}}'
			else:
				yield '{"response":{"error":"Sorry, Authora has no results for ' + topic + '. Our database is always growing, so check back soon or sign-up and repeat your search and we\'ll let you know when that data is available!"}}'

@route('/search/author/name')
def searchAuthoraByAuthorName():
	try:
		name = request.query.get('name')
		
	except Exception, ex:
		logging.exception('no name error: searchAuthoraByAuthorName()')
		name = None
	try:
		email = request.query.get('email')
	except Exception, ex:
		logging.exception('no email error: searchAuthoraByAuthorName()')
		email = None

	if name is None:
		yield '{"response":{"error":"Sorry, the server received a malformed request. Please refresh the page and try your search again."}}'
	else:
		try:
			name = urllib.unquote(name)
			name = unicode(name.strip(),errors='ignore')
			name = name.lower()
		except Exception, ex:
			logging.exception('name to unicode error: searchAuthoraByAuthorName()')
		
	
		resultList = content.find({u'authorSearch':name}).sort(u'totalShares', pymongo.DESCENDING).limit(100)
		resultCount = resultList.count()
		conn.end_request()
	
		if resultCount > 0:
			count = 0
			result = ''
			for record in resultList:
				authorID = record[u'authorID']
				authorName = record[u'authorName']
				url = record[u'url']
				urlNoHttp = url[7:]
				urlEncoded = urllib.quote_plus(url)
				urlNoHttp = urllib.quote_plus(urlNoHttp)
				shares = record[u'shares']
				result = result + authorID + ',' + authorName + ',' + url + ','
				result = result + 'http://www.linkdiagnosis.com/?q=' + urlEncoded + ',http://www.opensiteexplorer.org/links.html?no_redirect=1&page=1&site=' + url + ',http://www.majesticseo.com/reports/site-explorer?folder=&q=' + urlEncoded + '?IndexDataSource=H' 
				for item in shares:
					for key in item.keys():
						result = result + ',' + str(item[key]) + ''
				count = count + 1
				if count < resultCount:
					result = result + ';'
			result = result.rstrip(';')
			yield result
			
		else:
			if email is not None:
				try:
					email = urllib.unquote(email)
					email = unicode(email.strip(),errors='ignore')
					email = email.lower()
				except Exception, ex:
					logging.exception('name to unicode error: searchAuthoraByAuthorName()')

				yield '{"response":{"message":"Sorry, Authora has no results for ' + name + '. We will alert you by email when we have results for that request. Please check back soon!"}}'
			else:
				yield '{"response":{"error":"Sorry, Authora has no results for ' + name + '. Our database is always growing, so check back soon or sign-up and repeat your search and we\'ll let you know when that data is available!"}}'

@route('/search/author/account')
def searchAuthoraByAuthorAccount():
	try:
		account = request.query.get('account')
		
	except Exception, ex:
		logging.exception('no account error: searchAuthoraByAuthorAccount()')
		account = None
	try:
		email = request.query.get('email')
	except Exception, ex:
		logging.exception('no email error: searchAuthoraByAuthorAccount()')
		email = None

	if account is None:
		yield json.dumps({u'response':{u'error':unicode('Sorry, the server received a malformed request. Please refresh the page and try your search again.')}})
	else:
		try:
			account = urllib.unquote(account)
			account = unicode(account.strip(),errors='ignore')
			account = account.lower()
		except Exception, ex:
			logging.exception('account to unicode error: searchAuthoraByAuthorAccount()')
		
	
		resultList = authors.find({u'accounts': { u'$in' : [{u'facebook':account},{u'twitter':account},{u'linkedin':account},{u'gplus':account}]}})
		resultCount = resultList.count()
		conn.end_request()
		
		if resultCount > 0:
			#grabs last result is there are dupes
			for result in resultList:
				authorID = result[u'_id']
	
			if authorID is not None:
				resultList = content.find({u'authorID':str(authorID)}).sort(u'totalShares', pymongo.DESCENDING).limit(100)
				resultCount = resultList.count()
				conn.end_request()
		
				if resultCount > 0:
					count = 0
					result = ''
					for record in resultList:
						authorID = record[u'authorID']
						authorName = record[u'authorName']
						url = record[u'url']
						urlNoHttp = url[7:]
						urlEncoded = urllib.quote_plus(url)
						urlNoHttp = urllib.quote_plus(urlNoHttp)
						shares = record[u'shares']
						result = result + authorID + ',' + authorName + ',' + url + ','
						result = result + 'http://www.linkdiagnosis.com/?q=' + urlEncoded + ',http://www.opensiteexplorer.org/links.html?no_redirect=1&page=1&site=' + url + ',http://www.majesticseo.com/reports/site-explorer?folder=&q=' + urlEncoded + '?IndexDataSource=H' 
						for item in shares:
							for key in item.keys():
								result = result + ',' + str(item[key])
						count = count + 1
						if count < resultCount:
							result = result + ';'
					result = result.rstrip(';')
					yield result

				else:
					if email is not None:
						try:
							email = urllib.unquote(email)
							email = unicode(email.strip(),errors='ignore')
							email = email.lower()
						except Exception, ex:
							logging.exception('name to unicode error: searchAuthoraByAuthorName()')

						yield '{"response":{"message":"Sorry, Authora has no results for ' + account + '. We will alert you by email when we have results for that request. Please check back soon!"}}'
					else:
						yield '{"response":{"error":"Sorry, Authora has no results for ' + account + '. Our database is always growing, so check back soon or sign-up and repeat your search and we\'ll let you know when that data is available!"}}'
			
			else:
				if email is not None:
					try:
						email = urllib.unquote(email)
						email = unicode(email.strip(),errors='ignore')
						email = email.lower()
					except Exception, ex:
						logging.exception('name to unicode error: searchAuthoraByAuthorName()')

					yield '{"response":{"message":"Sorry, Authora has no results for ' + account + '. We will alert you by email when we have results for that request. Please check back soon!"}}'
				else:
					yield '{"response":{"error":"Sorry, Authora has no results for ' + account + '. Our database is always growing, so check back soon or sign-up and repeat your search and we\'ll let you know when that data is available!"}}'
							
		else:
			if email is not None:
				try:
					email = urllib.unquote(email)
					email = unicode(email.strip(),errors='ignore')
					email = email.lower()
				except Exception, ex:
					logging.exception('name to unicode error: searchAuthoraByAuthorName()')

				yield '{"response":{"message":"Sorry, Authora has no results for ' + account + '. We will alert you by email when we have results for that request. Please check back soon!"}}'
			else:
				yield '{"response":{"error":"Sorry, Authora has no results for ' + account + '. Our database is always growing, so check back soon or sign-up and repeat your search and we\'ll let you know when that data is available!"}}'
	
logger.exception('Authora API Starting on Port 8888');
run(host='0.0.0.0', port=8888, server='gevent')
