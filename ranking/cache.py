import gevent
import gevent.coros
import gevent.monkey
gevent.monkey.patch_all()

#import redis
import collections
import memcache
import time
import thread
from util import get_hash, HashRing
import util
import random

cache = None

hit = 0
total = 0

class LocalCache(object):
	#def __init__(self, size = 1000000, life = 60):
	def __init__(self, size = 1000000, life = 60):
		self.size = size
		self.life = life
		self.lru = collections.deque()
		self.m = {}

	def flush(self):
		t = time.time()
		while self.lru and (len(self.m) > self.size or t - self.lru[0][1] > self.life):
			k, _ = self.lru.popleft()
			mv = self.m[k]
			mv[1] -= 1
			if mv[1] == 0:
				del self.m[k]
	def __contains__(self, k):
		self.flush()
		return k in self.m

	def __getitem__(self, k):
		self.flush()
		if k in self.m:
			return self.m[k][0]
		return None
	def __setitem__(self, k, v):
		if self.lru and self.lru[-1][0] == k:
			self.lru[-1] = (k, time.time())
			self.m[k][0] = v
			return
		t = time.time()
		self.lru.append((k, t))
		if k in self.m:
			mv = self.m[k]
			mv[0] = v
			mv[1] += 1
		else:
			self.m[k] = [v, 1]
		self.flush()

class MemCachePool(object):
	def __init__(self, addr):
		self.addr = addr
		#self.pool = {}

		self.client = memcache.Client([self.addr], debug=True)
		self.client._statlog = lambda _:None
	def __str__(self):
		return "MemCachePool({0})".format(self.addr)
	def conn(self):
		#poolId = thread.get_ident()
		#if poolId not in self.pool:
			#self.pool[poolId] = memcache.Client([self.addr], debug=True)
		#return self.pool[poolId]

		return self.client


class Cache(object):
	def __init__(self, info):
		self.update(info)
		self.localCache = LocalCache()

	def ping(self):
		# TODO: ping cache servers and remove not responding servers
		pass
	
	def update(self, info):
		self.ring = HashRing()
		self.servers = {}
		for addr in info:
			#if addr in self.servers:
				#del old_servers[addr]
				#continue
			r = None
			#if ':' in addr:
				#host, port = addr.split(':')
				#r = redis.StrictRedis(host, port)
			#else:
				#host = addr
				#r = redis.StrictRedis(host)
			r = MemCachePool(addr)#memcache.Client([addr], debug=True)
			self.servers[addr] = r
			self.ring.add_node(r)
		#for k, r in old_servers.itervalues():
			#del self.servers[k]
			#self.ring.remove_node(r)

	def mget(self, keys):
		_t = time.time()
		global hit, total
		res = {}
		split = {}
		for key in keys:
			total += 1
			if key in self.localCache:
				ret = self.localCache[key]
				if ret is not None:
					res[key] = ret
					continue
			split.setdefault(self.ring.find_node(key), []).append(key)
		sem = gevent.coros.Semaphore(0)
		print 'CACHE_BEFORE_SPLIT',time.time()-_t
		for node, subKeys in split.iteritems():
			#print 'MGET', node, len(subKeys)
			def helper(localCache, node, subKeys, res):
				print 'CACHE_DOING', id(node), time.time()-_t
				ret = node.conn().get_multi(subKeys)
				print 'CACHE_DONE', id(node), time.time()-_t
				for k, v in ret.iteritems():
					localCache[k] = v
				res.update(ret)
				sem.release()
			util.tasks.put((helper, self.localCache, node, subKeys, res))
		print 'CACHE_AFTER_SPLIT',time.time()-_t
		for _ in split:
			sem.acquire()
		print 'CACHE_AFTER_ACQUIRE',time.time()-_t
		hit += len(res)
		if random.randrange(100) == 0:
			print 'HITRATE', hit*100/total
		return res
					
	def get(self, key):
		#print 'cache.get'
		#tt = time.time()
		if key in self.localCache:
			ret = self.localCache[key]
			if ret is not None:
				return ret
		#print time.time() - tt, 1
			
		global hit, total
		r = self.ring.find_node(key).conn()
		#print time.time() - tt, 2
		ret = r.get(key)
		#print time.time() - tt, 3
		total += 1
		if ret is not None:
			hit += 1
			self.localCache[key] = ret
		else:
			if random.randrange(100) == 0:
				print 'MISS', hit*100/total, key, self.ring.find_node(key)
			pass
		#print time.time() - tt, 4
		return ret
		
	def put(self, key, value, timelimit = None):
		r = self.ring.find_node(key).conn()
		self.localCache[key] = value
		if timelimit is None:
			ret = r.set(key, value)
		else:
			ret = r.set(key, value, timelimit)
		if ret == 0:
			print "CACHE_PUT FAIL ", self.ring.find_node(key)

def init(info):
	global cache
	if cache:
		cache.update(info)
	else:
		cache = Cache(info)

def mget(keys):
	if cache is None:
		return None
	return cache.mget(keys)
def get(key):
	#print 'CACHE GET',key,
	if cache is None:
		return None
	return cache.get(str(key))

def put(key, value, timelimit = None):
	#print 'CACHE PUT',key,value
	if cache is None:
		return
	cache.put(str(key), str(value), timelimit)
