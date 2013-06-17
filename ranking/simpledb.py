# encoding: utf-8

import multiprocessing

import gevent
import gevent.monkey
import gevent.queue
import gevent.coros
gevent.monkey.patch_all()


import thread
import boto
import boto.sdb as sdb

import random
import bisect
import time
import sys

import cache
import config
from util import get_hash, HashRing, build_key, backoff, parse_key, init_workers
import util

verbose_ = True

REPLICATION_COUNT = 16

conn = boto.sdb.connect_to_region(config.region,
	aws_access_key_id=config.access_key,
	aws_secret_access_key=config.secret_key,
	is_secure=False
)

def get_last_reset_time(g, t = None):
	if t is None:
		t = time.time()
	base = int(g['b'])
	interval = int(g['i'])
	return t - (t - base) % interval

def get_current_score(g, score, t, lt = None):
	if lt is None:
		lt = get_last_reset_time(g)
	if int(t) > lt:
		return int(score)
	return 0

def DomainCreator(preCreateDomainName, mature):
	def CreateDomainHelp():
		conn.create_domain(preCreateDomainName)
		gevent.sleep()
		conn.get_domain(preCreateDomainName)
	backoff(CreateDomainHelp, boto.exception.SDBResponseError)
	for idx in xrange(REPLICATION_COUNT):
		backoff(lambda:self.master_domain.put_attributes(get_hash(preCreateDomainName+':%x'%idx), dict(domain=preCreateDomainName, mature=mature)),
			boto.exception.SDBResponseError)
	print 'creating', preCreateDomainName, 'done'

class DomainProxy(boto.sdb.domain.Domain):
	def __init__(self, dom, mature):
		boto.sdb.domain.Domain.__init__(self, dom.connection, dom.name)
		self.mature = mature

class DB(object):
	def __init__(self, isTest = True):
		self.gameCache = {}
		if isTest:
			self.master_domain_name = 'master_test'
			self.prefix = 'node_test'
			self.cache_domain_name = 'cache_test'
			self.game_domain_name = 'game_test'
			self.domain_count = self.initial_domain_count = 4
		else:
			self.master_domain_name = 'master'
			self.prefix = 'node'
			self.cache_domain_name = 'cache'
			self.game_domain_name = 'game'
			self.domain_count = self.initial_domain_count = 200
		self.game_domain = conn.create_domain(self.game_domain_name)
		try:
			self.master_domain = conn.get_domain(self.master_domain_name)
		except boto.exception.SDBResponseError:
			self.master_domain = conn.create_domain(self.master_domain_name)
			gevent.sleep(0.1)
			print 'prepare db'
			for i in xrange(self.domain_count):
				preCreateDomainName = self.prefix + '%04x'%i

				print 'task creating', preCreateDomainName
				tasks.put((DomainCreator, preCreateDomainName, 1))
				gevent.sleep()
			tasks.join()

		self.load_servers()
		self.load_caches()

	def load_caches(self):
		#cache.init(['ranking-cache-small.ispvtc.0001.apne1.cache.amazonaws.com:11211'])
		cache.init([
	'ranking-cache-small.ispvtc.0001.apne1.cache.amazonaws.com:11211',
	'ranking-cache-small.ispvtc.0002.apne1.cache.amazonaws.com:11211',
	'ranking-cache-small.ispvtc.0003.apne1.cache.amazonaws.com:11211',
	'ranking-cache-small.ispvtc.0004.apne1.cache.amazonaws.com:11211',
	'ranking-cache-small.ispvtc.0005.apne1.cache.amazonaws.com:11211',
	'ranking-cache-small.ispvtc.0006.apne1.cache.amazonaws.com:11211',
	'ranking-cache-small.ispvtc.0007.apne1.cache.amazonaws.com:11211',
	'ranking-cache-small.ispvtc.0008.apne1.cache.amazonaws.com:11211',
])
		#cache.init(['192.168.0.4:11211'])

	def load_servers(self):
		self.servers = []
		dom = self.master_domain
		domains = {}
		t1 = time.time()
		if verbose_:
			print 'load_servers'

		for item in dom.select('select * from %s' % self.master_domain_name, consistent_read = True):
			mature = 1
			if 'mature' in item:
				mature = int(item['mature'])
			if item['domain'] not in domains:
				domains[item['domain']] = DomainProxy(conn.get_domain(item['domain'], validate=False), mature)
			self.servers.append((item.name, item['domain'], domains[item['domain']]))

		self.servers.sort()

		assert self.servers, "server should not empty"
		assert len(self.servers) >= self.domain_count * REPLICATION_COUNT
		self.domain_count = len(self.servers) / REPLICATION_COUNT
		#if verbose_:
			#print '\n'.join(str(x) for x in self.servers)
		if verbose_:
			print 'load_servers done: domain count %d' % self.domain_count, time.time() - t1

	def find_domain_for_key(self, key, step=0):
		assert self.servers > 0
		h = get_hash(key)
		pos = bisect.bisect_left(self.servers, (h, None))+step
		while pos >= len(self.servers):
			pos -= len(self.servers)
		return self.servers[pos][2]

	def iter_split_keys_by_domain(self, keys, step=0):
		for k in keys:
			dom = self.find_domain_for_key(k, step)
			yield dom, k

	def split_keys_by_domain(self, game_id, keys, step=0):
		t1 = time.time()
		result = {}
		for k in keys:
			dom = self.find_domain_for_key(build_key(game_id, k), step)
			if dom not in result:
				result[dom] = []
			result[dom].append(k)
		return result

	def cache_user_score(self, game_id, u_id, score, time):
		data = str('%s %s' % (score, time))
		cache.put(build_key(game_id, u_id)+'s', data, 86400*7)

	def cache_user_friend(self, game_id, u_id, friends):
		data = ','.join(friends)
		cache.put(build_key(game_id, u_id)+'f', data, 86400*7)

	def cache_user(self, game_id, u_id, user):
		self.cache_user_score(game_id, u_id, user['s'], user['t'])
		if 'f' in user:
			self.cache_user_friend(game_id, u_id, user['f'])

	def get_user_friends(self, game_id, u_id, only_from_cache=False):
		k = build_key(game_id, u_id)
		c = cache.get(k+'f')
		if c:
			return c.split(',')
		if c == '':
			return []
		if only_from_cache:
			return None
		user = self.get_user(game_id, u_id)
		return user['f']

	def get_user_score(self, game_id, u_id, only_from_cache=False, prefetch = None):
		g = self.get_game(game_id)
		if g is None:
			return 0
		k = build_key(game_id, u_id)
		c = None
		if prefetch and k+'s' in prefetch:
			c = prefetch[k+'s']
		if c is None:
			c = cache.get(k+'s')
		if c:
			s, t = c.split(' ')
			return get_current_score(g, s, int(t))
		if only_from_cache:
			return None
		user = self.get_user(game_id, u_id)
		if user is None:
			return 0
		return get_current_score(g, user['s'], int(user['t']))

	def put_game(self, game_id, resetBase, resetInterval, key = None):
		g = self.get_game(game_id)
		if g is not None and g['k'] != key:
			return
		if g is None:
			r = get_hash(str(random.random()))
		else:
			r = g['k']
		self.game_domain.put_attributes(game_id, {'b':resetBase, 'i':resetInterval, 'k':r})
		cache.put(game_id + 'g', '%d %d %s' % (resetBase, resetInterval, r))
		return r
		
	def get_game(self, game_id):
		if game_id in self.gameCache:
			if self.gameCache[game_id]['t'] - time.time() < 30*60:
				return self.gameCache[game_id]
		g = cache.get(game_id + 'g')
		if g is None:
			g = self.game_domain.get_item(game_id)
			print self.game_domain ,g
			if g is None:
				return g
			resetBase = int(g['b'])
			resetInterval = int(g['i'])
			r = g['k']
			cache.put(game_id + 'g', '%d %d %s' % (resetBase, resetInterval, r))
			g['t'] = time.time()
		else:
			b,i,k = g.split(' ',2)
			b = int(b)
			i = int(i)
			g = dict(b=b,i=i,k=k)
			g['t'] = time.time()
		self.gameCache[game_id] = g
		return g


	def get_user(self, game_id, u_id, with_domain = False):
		k = build_key(game_id, u_id)
		if not with_domain:
			cached_score = cache.get(k+'s')
			cached_friend = cache.get(k+'f')
			if cached_score and cached_friend:
				u = {}
				u['s'], u['t'] = cached_score.split()
				if cached_friend == '':
					u['f'] = []
				else:
					u['f'] = cached_friend.split(',')
				return u
		step = 0
		while 1:
			dom = self.find_domain_for_key(k, step)
			try:
				item = dom.get_item(k)
				if item is not None:
					if 'f' not in item:
						item['f'] = []
					if 's' not in item:
						item['s'] = 0
					if 't' not in item:
						item['t'] = int(time.time())
					friends = ''
					if type(item['f']) == list:
						friends = ','.join(item['f'])
					else:
						friends = item['f']
					item = dict(s=int(item['s']),t=int(item['t']),f=friends.split(','))
					self.cache_user(game_id, u_id, item)
					if with_domain:
						return item, dom
					else:
						return item
			except boto.exception.SDBResponseError:
				continue
			if not dom.mature:
				step += 1
				continue
			else:
				dom = db.find_domain_for_key(k)
				add_user(game_id, u_id)
				if with_domain:
					return dict(s=0,t=int(time.time()),f=[]), dom
				else:
					return dict(s=0,t=int(time.time()),f=[])

def setShardingCount(n):
	old_domain_count = db.domain_count
	db.domain_count = max(n, db.domain_count)
	for idx in xrange(old_domain_count, db.domain_count):
		tasks.put((DomainCreator,db.prefix+'%04x'%idx,0))
	tasks.join()
	db.load_servers()


def init(isTest = True, workerCount = 50, verbose = False):
	global db,verbose_, tasks
	verbose_ = verbose
	util.init_workers(workerCount)
	tasks = util.tasks
	db = DB(isTest = isTest)

def get_game(game_id):
	return db.get_game(game_id)

def put_game(game_id, resetBase, resetInterval, key=None):
	return db.put_game(game_id, resetBase, resetInterval, key)

def add_user(game_id,u_id):
	g = get_game(game_id)
	if g is None:
		return
	k = build_key(game_id, u_id)
	dom = db.find_domain_for_key(k)
	r = dom.put_attributes(k, {'s':0,'t':int(time.time())})
	if r:
		db.cache_user_score(game_id, u_id, 0, int(time.time()))
	return r

def update_friends(game_id, u_id, f_ids):
	g = get_game(game_id)
	if g is None:
		return False
	k = build_key(game_id,u_id)
	dom = db.find_domain_for_key(k)
	user, old_dom = db.get_user(game_id, u_id, with_domain = True)
	friends = []
	merged = []
	mergedLength = 0

	for f_id in f_ids:
		merged.append('%x'%f_id)
		mergedLength += len(merged[-1])
		if mergedLength + len(merged) > 950:
			friends.append(','.join(merged))
			merged = []
			mergedLength = 0
	if merged:
		friends.append(','.join(merged))
	if dom.name == old_dom.name:
		if friends:
			backoff(lambda:dom.put_attributes(k, {'f':friends}), boto.exception.SDBResponseError)
	else:
		backoff(lambda:dom.put_attributes(k, {'f':friends, 's':user['s'], 't':user['t']}), boto.exception.SDBResponseError)
	db.cache_user_friend(game_id, u_id, friends)
	return True

def get_ranking(game_id, u_id):
	return get_ranking_from(game_id, u_id, u_id)

def get_friend_score_list(game_id, u_id):
	print 
	print 'get_friend_score_list'
	friendStartTime = time.time()
	g = get_game(game_id)
	if g is None:
		return []
	t = int(time.time())
	t1 = time.time()
	user = db.get_user(game_id, u_id)
	if user is None:
		return []
	f_ids = db.get_user_friends(game_id, u_id)
	if f_ids is None:
		return []
	# TEST
	#print 'TEST: limiting friends to 100'
	#f_ids = f_ids[:100]
	scores = dict((f, 0) for f in f_ids)
	scores['%x'%u_id] = db.get_user_score(game_id, u_id)
	step = 0
	#print time.time() - friendStartTime, "START", len(f_ids)
	debug_fc = 0
	debug_fc2 = 0
	print 'BEFORE_FOR', time.time() - friendStartTime
	cacheData = cache.mget(build_key(game_id, x)+'s' for x in f_ids)
	qn = [0,0]
	while f_ids:
		proceed = set()
		sem = gevent.coros.Semaphore(1)
		createTaskSem = gevent.coros.Semaphore(1)
		subTasks = []
		print 'BEFORE_SPLIT', time.time() - friendStartTime
		splitResult = db.split_keys_by_domain(game_id, f_ids, step).iteritems()
		print 'AFTER_SPLIT', time.time() - friendStartTime
		for dom, friends_all in splitResult:
			debug_fc+=len(friends_all)
			#print 'FOR', dom, len(friends_all), time.time() - friendStartTime
			def helper(dom, part):
				#print 'helper',dom,part,time.time()-friendStartTime
				query = 'select s,t from %s where ' % dom.name
				#query += '(' +  ' or '.join("itemName() = '%s'" % build_key(game_id,f) for f in part) + ')'
				query += 'itemName() in (' +  ', '.join("'%s'" % build_key(game_id,f) for f in part) + ')'
				def select_try():
					qn[0] += 1
					t=time.time()
					tt=t
					select_result = dom.select(query)
					for f in select_result:
						t2=time.time()
						#print 'select_try',f,f.name,parse_key(f.name)[1]
						scores[parse_key(f.name)[1]] = get_current_score(g, f['s'], f['t'])
						proceed.add(parse_key(f.name)[1])
						sem.release()
						db.cache_user_score(game_id, parse_key(f.name)[1], f['s'], f['t'])
						t += time.time() - t2
					qn.append((time.time() - t, time.time() - tt))
				# if all data comes from cache, no need to query
				#if '()' not in query:
				backoff(select_try, boto.exception.SDBResponseError)
				if dom.mature:
					for f in part:
						if f not in proceed:
							proceed.add(f)
							if f not in scores:
								scores[f] = 0
							sem.release()
							db.cache_user_score(game_id, f, 0, int(time.time()))
			partSize = 20
			def create_task_helper(friends_all, dom, proceed, sem, createTaskSem, subTasks):
				#print 'create_task_helper', time.time() - friendStartTime
				#create_task_helpertime = time.time()
				collectedPartsNotInCache = []
				for f in friends_all:
					f_score = db.get_user_score(game_id, f, only_from_cache = True, prefetch = cacheData)
					if f_score is None:
						collectedPartsNotInCache.append(f)
					else:
						qn[1] += 1
						scores[f] = f_score
						proceed.add(f)
						sem.release()
						createTaskSem.release()
					if len(collectedPartsNotInCache) == partSize:
						subTasks.append((helper, dom, collectedPartsNotInCache))
						for f in collectedPartsNotInCache:
							createTaskSem.release()
						collectedPartsNotInCache = []
					#gevent.sleep(0)
				if collectedPartsNotInCache:
					#tasks.put((helper, dom, collectedPartsNotInCache))
					subTasks.append((helper, dom, collectedPartsNotInCache))
					for f in collectedPartsNotInCache:
						createTaskSem.release()
					collectedPartsNotInCache = []
				#print 'create_task_helper log', time.time() - create_task_helpertime, len(friends_all)
				#print 'create_task_helper end', time.time() - friendStartTime
			tasks.put((create_task_helper, friends_all, dom, proceed, sem, createTaskSem, subTasks))
			#tasks.put((helper, dom, collectedPartsNotInCache))
		print time.time() - friendStartTime, "CREATE LOOP END", len(f_ids)
		subTasksCount = 0
		for _ in f_ids:
			createTaskSem.acquire()
			while subTasks:
				x = subTasks.pop()
				subTasksCount += 1
				tasks.put(x)
		print time.time() - friendStartTime, "LOOP END", len(f_ids), subTasksCount
		for _ in f_ids:
			sem.acquire()
		print time.time() - friendStartTime, "ACQUIRE END", len(f_ids)
		#tasks.join()
		f_ids = list(set(f_ids) - proceed)
		step += 1
		if step > 5:
			break
		if f_ids:# and verbose_:
			print time.time() - friendStartTime, "RELOOP"
			print 'not proceed',len(proceed), len(f_ids), f_ids[0]

	conn.print_usage()
	print time.time() - friendStartTime, "FINAL"
	return scores.items()

def get_ranking_from(game_id, u_id, view_u_id):
	g = get_game(game_id)
	if g is None:
		return 1
	t = int(time.time())
	t1 = time.time()
	user = db.get_user(game_id, u_id)
	if user is None:
		return 1
	score = db.get_user_score(game_id, u_id)
	if score is None:
		return 1
	scores = get_friend_score_list(game_id, view_u_id)
	return 1 + sum(1 for x, y in scores if y > score)

def multiple_get_ranking_from(game_id, u_id, froms):
	g = get_game(game_id)
	if g is None:
		return [[f, 1] for f in froms]
	rankings = []
	def helper(game_id, u_id, view_id):
		rankings.append([view_id, get_ranking_from(game_id, u_id, view_id)])
	for f in froms:
		helper(game_id, u_id, f)
		#tasks.put((helper, game_id, u_id, f))
		#gevent.sleep()
	tasks.join()
	return rankings

def update_score(game_id, u_id, score, forced = False):
	startTime = time.time()
	g = get_game(game_id)
	if g is None:
		return False, 0
	t = int(time.time())
	k = build_key(game_id, u_id)
	step = 0
	while 1:
		print time.time() - startTime, step
		user, old_dom = db.get_user(game_id, u_id, with_domain = True)
		if user is None:
			return False, 0
		dom = db.find_domain_for_key(k)

		oldt = user['t']
		#if '.' in user['t']:
			#user['t'] = str(int(float(user['t'])))
		currentScore = get_current_score(g, user['s'], user['t'])
		if currentScore < score or forced:
			try:
				print time.time() - startTime, step, 'before put'
				if old_dom.name != dom.name:
					dom.put_attributes(k, {'s':score, 't':t, 'f':user['f']})
				else:
					dom.put_attributes(k, {'s':score, 't':t}, expected_value=['t',oldt])
				print time.time() - startTime, step, 'before cache'
				db.cache_user_score(game_id, u_id, score, t)
				print time.time() - startTime, step, 'return'
				return True, score
			except boto.exception.SDBResponseError as e:
				#print e
				gevent.sleep(.1 + 0.05 * random.randrange(1<<min(8,step)))
				pass
		else:
			return False, currentScore
		step += 1

