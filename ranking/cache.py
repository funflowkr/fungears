import gevent
import gevent.monkey
gevent.monkey.patch_all()

#import redis
import memcache
from util import get_hash, HashRing

cache = None

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
			r = memcache.Client([addr], debug=True)
			self.servers[addr] = r
			self.ring.add_node(r)
		for k, r in old_servers.itervalues():
			del self.servers[k]
			self.ring.remove_node(r)

	def get(self, key):
		r = self.ring.find_node(key)
		return r.get(key)
		
	def put(self, key, value, timelimit = None):
		r = self.ring.find_node(key)
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
