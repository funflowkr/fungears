import gevent
import gevent.monkey
gevent.monkey.patch_all()

#import redis
import memcache
import thread
from util import get_hash, HashRing

cache = None

class MemCachePool(object):
	def __init__(self, addr):
		self.addr = addr
		self.pool = {}
	def __str__(self):
		return "MemCachePool({})".format(self.addr)
	def conn(self):
		poolId = thread.get_ident()
		if poolId not in self.pool:
			self.pool[poolId] = memcache.Client([addr], debug=True)
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
		r = self.ring.find_node(key).conn()
		return r.get(key)
		
	def put(self, key, value, timelimit = None):
		r = self.ring.find_node(key).conn()
		if timelimit is None:
			r.set(key, value)
		else:
			r.set(key, value, timelimit)

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
