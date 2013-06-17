#addr = 'ec2-54-249-172-147.ap-northeast-1.compute.amazonaws.com'
addr = 'localhost'

import urllib
import httplib
import json

def call(method, url, data = None):
	c = httplib.HTTPConnection(addr)
	headers = {}
	if type(data) == dict:
		data = json.dumps(data)
		headers = {"Content-type": "application/json",
		            "Accept": "text/plain"}
	
	c.request(method, url, data, headers)
	r = c.getresponse()
	return r.status, r.read()

key5sec = 'b6989d16d1684e82be6c35ea811e1065ecc16689'
def test_game_change():
	call('POST', '/game/5sec', dict(base=0, interval=3*60))
	assert json.loads(call('GET', '/game/5sec')[1])['interval'] == 30
	call('POST', '/game/5sec', dict(base=0, interval=3*60, secret=key5sec))
	assert json.loads(call('GET', '/game/5sec')[1])['interval'] == 3*60
	call('POST', '/game/5sec', dict(base=0, interval=30, secret=key5sec))
	assert json.loads(call('GET', '/game/5sec')[1])['interval'] == 30
#test_game_change()

def test_no_error_on_not_exists_users():
	print call('GET', '/friend_scores/5sec/10000', dict(secret=key5sec))
#test_no_error_on_not_exists_users()

def ranking_test():
	# 0's friend: 1,2
	# 1's friend: 2
	call("POST", '/update_score/5sec/0/14', dict(secret=key5sec))
	call("POST", '/update_score/5sec/1/13', dict(secret=key5sec))
	call("POST", '/update_score/5sec/2/12', dict(secret=key5sec))
	print call('GET', '/friend_scores/5sec/0', dict(secret=key5sec))
	print call('GET', '/friend_scores/5sec/1', dict(secret=key5sec))
	print call('GET', '/friend_scores/5sec/2', dict(secret=key5sec))
	print call('GET', '/ranking_from/5sec/2', dict(secret=key5sec, view_from=[0,1]))

#print call('POST', '/update_friends/5sec/0', dict(secret=key5sec, friends=[1,2]))
#ranking_test()
#print call('GET', '/debug_get_scores_and_ranking/5sec/0', dict(secret=key5sec))

def update_force_test():
	#call("POST", '/update_friends/5sec/3', dict(secret=key5sec, friends=[]))
	call("POST", '/update_score/5sec/3/14', dict(secret=key5sec))
	print call('GET', '/friend_scores/5sec/3', dict(secret=key5sec))
	call("POST", '/update_score/5sec/3/12', dict(secret=key5sec, forced=False))
	print call('GET', '/friend_scores/5sec/3', dict(secret=key5sec))
	call("POST", '/update_score/5sec/3/12', dict(secret=key5sec, forced=True))
	print call('GET', '/friend_scores/5sec/3', dict(secret=key5sec))

#update_force_test()
	
#print call('GET', '/game/stress')
key = 'e5ce29d42a93fa8cb45b542066c9517240e00f06'
print call('GET', '/ranking_from/stress/0', dict(secret=key, view_from=[4]*1))
