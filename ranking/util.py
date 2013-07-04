import hashlib
import sys
import random
import gevent
import gevent.queue
import bisect

tasks = None

def init_workers(workerCount):
	global tasks
	if tasks is not None:
		return
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
	

def backoff(f, exc, max_try_count = None):
	try_count = 0
	while 1:
		try:
			return True, f()
			break
		except exc as e:
			print e
			gevent.sleep(0.05 + 0.05 * random.randrange(1 << min(8,try_count)))
			try_count += 1
		if max_try_count and try_count > max_try_count:
			return False, 

def get_hash(s):
	#return hashlib.sha1("PrEfIx|"+s).digest().encode('hex')
	return hashlib.sha1(s).digest().encode('hex')

def parse_key(k):
	if '|' in k:
		return k.split('|')
	return None, k

def build_key(g_id, u_id):
	if isinstance(u_id, int):
		u_id = '%x' % u_id
	if g_id is None:
		return u_id
	else:
		return "%s|%s"%(g_id, u_id)

class HashRing(object):
	def __init__(self, nodes = None, replicas = 16):
		self.replicas = replicas

		self.ring = {}
		self.sorted_keys = []
		self.nodes = []

		if nodes:
			for node in nodes:
				self.add_node(node)

	def add_node(self, node):
		self.nodes.append(node)
		for i in xrange(self.replicas):
			key = get_hash('%s:%s'%(node, i))
			self.ring[key] = node
			self.sorted_keys.append(key)
		self.sorted_keys.sort()
		#for x in self.sorted_keys:
			#print x, self.ring[x]

	def remove_node(self, node):
		assert node in self.nodes
		self.nodes.remove(node)
		for i in xrange(self.replicas):
			key = get_hash('%s:%s'%(node, i))
			del self.ring[key]
			self.sorted_keys.remove(key)

	def find_node_pos(self, key):
		h = get_hash(key)
		pos = bisect.bisect_left(self.sorted_keys, h)
		while pos >= len(self.nodes):
			pos -= len(self.nodes)
		return pos

	def find_node(self, key):
		return self.ring[self.sorted_keys[self.find_node_pos(key)]]

	def gen_nodes(self, key):
		pos = self.find_node_pos(key)
		last_node = None
		while 1:
			current_node = self.ring[self.sorted_keys[pos]]
			if current_node != last_node:
				yield current_node
				last_node = current_node
			pos += 1
			if pos == len(self.sorted_keys):
				pos = 0
