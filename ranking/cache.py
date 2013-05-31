import gevent
import gevent.monkey
gevent.monkey.patch_all()

#import redis
import memcache
import thread
from util import get_hash, HashRing

cache = None

hit = 0
total = 0

class MemCachePool(object):
	def __init__(self, addr):
		self.addr = addr
		self.pool = {}
	def __str__(self):
		return "MemCachePool({0})".format(self.addr)
	def conn(self):
		poolId = thread.get_ident()
		if poolId not in self.pool:
			self.pool[poolId] = memcache.Client([self.addr], debug=True)
		return self.pool[poolId]


class Cache(object):
	def __init__(self, info):
		self.servers = {}
		self.ring = HashRing()
		self.update(info)

	def ping(self):
		# TODO: ping cache servers and remove not responding servers
		pass
	
	def update(self, info):
		old_servers = self.servers.copy()
		for addr in info:
			if addr in self.servers:
				del old_servers[addr]
				continue
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
		for k, r in old_servers.itervalues():
			del self.servers[k]
			self.ring.remove_node(r)

	def get(self, key):
		global hit, total
		r = self.ring.find_node(key).conn()
		ret = r.get(key)
		total += 1
		if ret is not None:
			hit += 1
		else:
			print 'MISS', hit*100/total, key, self.ring.find_node(key)
		return ret
		
	def put(self, key, value, timelimit = None):
		r = self.ring.find_node(key).conn()
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

def get(key):
	if cache is None:
		return None
	return cache.get(str(key))

def put(key, value, timelimit = None):
	if cache is None:
		return
	cache.put(str(key), str(value), timelimit)
