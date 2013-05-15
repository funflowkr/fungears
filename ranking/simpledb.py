import gevent
import gevent.monkey
import gevent.queue
gevent.monkey.patch_all()

import boto
import boto.sdb as sdb

import random
import bisect
import time
import sys

import cache
import config
from util import get_hash, HashRing, build_key, backoff, parse_key

verbose_ = True

REPLICATION_COUNT = 16

conn = boto.sdb.connect_to_region(config.region,
	aws_access_key_id=config.access_key,
	aws_secret_access_key=config.secret_key)

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
		gevent.sleep(1)
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
		cache.init(['ranking-cache-test.ispvtc.0001.apne1.cache.amazonaws.com:11211'])
		#cache.init(['192.168.0.4:11211'])

	def load_servers(self):
		self.servers = []
		dom = self.master_domain
		domains = {}
		t1 = time.time()
		if verbose_:
			print 'load_servers'

		for item in dom.select('select * from %s' % self.master_domain_name, consistent_read = True):
			if item['domain'] not in domains:
				domains[item['domain']] = conn.get_domain(item['domain'], validate=False)
			mature = 1
			if 'mature' in item:
				mature = int(item['mature'])
			self.servers.append((item.name, item['domain'], DomainProxy(domains[item['domain']], mature)))

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
		if only_from_cache:
			return None
		user = self.get_user(game_id, u_id)
		return ','.join(user['f']).split(',')

	def get_user_score(self, game_id, u_id, only_from_cache=False):
		g = self.get_game(game_id)
		if g is None:
			return 0
		k = build_key(game_id, u_id)
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
			if key is None:
				r = get_hash(str(random.random()))
			else:
				r = key
		else:
			r = g['k']
		self.game_domain.put_attributes(game_id, {'b':resetBase, 'i':resetInterval, 'k':r})
		cache.put(game_id + 'g', {'b':resetBase, 'i':resetInterval, 'k':r})
		return r
		
	def get_game(self, game_id):
		g = cache.get(game_id + 'g')
		if g is None:
			g = self.game_domain.get_item(game_id)
			if g is None:
				return g
			resetBase = g['b']
			resetInterval = g['i']
			r = g['k']
			cache.put(game_id + 'g', {'b':resetBase, 'i':resetInterval, 'k':r})
		return g


	def get_user(self, game_id, u_id, with_domain = False):
		k = build_key(game_id, u_id)
		if not with_domain:
			cached_score = cache.get(k+'s')
			cached_friend = cache.get(k+'f')
			if cached_score and cached_friend:
				u = {}
				u['s'], u['t'] = cached_score.split()
				u['f'] = cached_friend
				return u
		step = 0
		while 1:
			dom = self.find_domain_for_key(k, step)
			try:
				item = dom.get_item(k)
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
				if with_domain:
					return None, dom
				else:
					return None

def setShardingCount(n):
	old_domain_count = db.domain_count
	db.domain_count = max(n, db.domain_count)
	for idx in xrange(old_domain_count, db.domain_count):
		tasks.put((DomainCreator,db.prefix+'%04x'%idx,0))
	tasks.join()
	db.load_servers()


def init(isTest = True, workerCount = 200, verbose = False):
	global db,tasks, verbose_
	verbose_ = verbose
	tasks = gevent.queue.JoinableQueue(workerCount*2)
	for i in xrange(workerCount):
		def worker():
			while True:
				item = tasks.get()
				try:
					item[0](*item[1:])
				except Exception, e:
					print >>sys.stderr, e
					raise
				tasks.task_done()

		gevent.spawn(worker)
	db = DB(isTest = isTest)

def get_game(game_id):
	return db.get_game(game_id)

def put_game(game_id, resetBase, resetInterval, key=None):
	db.put_game(game_id, resetBase, resetInterval, key)

def add_user(game_id,u_id):
	g = get_game(game_id)
	if g is None:
		return
	k = build_key(game_id, u_id)
	dom = db.find_domain_for_key(k)
	dom.put_attributes(k, {'s':0,'t':int(time.time())})

def add_friends(game_id, u_id, f_ids):
	g = get_game(game_id)
	if g is None:
		return
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
		backoff(lambda:dom.put_attributes(k, {'f':friends}), boto.exception.SDBResponseError)
	else:
		backoff(lambda:dom.put_attributes(k, {'f':friends, 's':user['s'], 't':user['t']}), boto.exception.SDBResponseError)

def get_ranking(game_id, u_id):
	return get_ranking_from(game_id, u_id, u_id)

def get_friend_score_list(game_id, u_id):
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
	scores = {}
	scores['%x'%u_id] = db.get_user_score(game_id, u_id)
	step = 0
	while f_ids:
		proceed = set()
		for dom, friends_all in db.split_keys_by_domain(game_id, f_ids, step).iteritems():
			print 'after split',dom,friends_all
			def helper(dom, part):
				query = 'select s,t from %s where ' % dom.name
				query += '(' +  ' or '.join("itemName() = '%s'" % build_key(game_id,f) for f in part) + ')'
				def select_try():
					for f in dom.select(query):
						print 'select_try',f,f.name,parse_key(f.name)[1]
						scores[parse_key(f.name)[1]] = get_current_score(g, f['s'], f['t'])
						proceed.add(parse_key(f.name)[1])
						db.cache_user_score(game_id, parse_key(f.name)[1], f['s'], f['t'])
				# if all data comes from cache, no need to query
				print dom,query
				#if '()' not in query:
				backoff(select_try, boto.exception.SDBResponseError)
				if dom.mature:
					for f in part:
						proceed.add(f)
			partSize = 20
			collectedPartsNotInCache = []
			for f in friends_all:
				f_score = db.get_user_score(game_id, f, only_from_cache = True)
				if f_score is None:
					collectedPartsNotInCache.append(f)
				else:
					scores[f] = f_score
					proceed.add(f)
				if len(collectedPartsNotInCache) == partSize:
					tasks.put((helper, dom, collectedPartsNotInCache))
					collectedPartsNotInCache = []
				gevent.sleep(0)
			if collectedPartsNotInCache:
				tasks.put((helper, dom, collectedPartsNotInCache))
				collectedPartsNotInCache = []
		tasks.join()
		f_ids = list(set(f_ids) - proceed)
		step += 1
		if step > 5:
			break
		if f_ids and verbose_:
			print 'not proceed',len(proceed), len(f_ids), f_ids[0]

	print scores
	return scores.items()

def get_ranking_from(game_id, u_id, view_u_id):
	g = get_game(game_id)
	if g is None:
		return
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

def update_score(game_id, u_id, score):
	g = get_game(game_id)
	if g is None:
		return
	t = int(time.time())
	k = build_key(game_id, u_id)
	while 1:
		user, old_dom = db.get_user(game_id, u_id, with_domain = True)
		if user is None:
			return False
		dom = db.find_domain_for_key(k)
		print k, dom

		oldt = user['t']
		if '.' in user['t']:
			user['t'] = str(int(float(user['t'])))
		if get_current_score(g, int(user['s']), user['t']) < score and int(user['t']) < t:
			try:
				if old_dom.name != dom.name:
					dom.put_attributes(k, {'s':score, 't':t, 'f':user['f']})
					print k,score,t,dom
				else:
					dom.put_attributes(k, {'s':score, 't':t}, expected_value=['t',oldt])
					print k,score,t,dom
				db.cache_user_score(game_id, u_id, score, t)
				return True
			except boto.exception.SDBResponseError as e:
				#print e
				gevent.sleep(.1)
				pass
		else:
			return False

