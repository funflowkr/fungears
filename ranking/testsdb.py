import gevent
import boto
import gevent.monkey
gevent.monkey.patch_all()
import simpledb as s
import random
import time
import random
import gevent.queue
isTest = True
s.init(isTest=isTest, verbose = True)

s.conn.print_usage()
N = 1000000 if not isTest else 100
FN = 500 if not isTest else 50
tasks = gevent.queue.JoinableQueue()
for i in xrange(100):
	def worker():
		while True:
			item = tasks.get()
			try:
				item[0](*item[1:])
			except Exception, e:
				print e
				raise
			tasks.task_done()
	gevent.spawn(worker)
# add 1000 user : 0.0003$
def create_users():
	t1 = time.time()
	print 'add_user'
	cc = [0]
# optimized loop
	bare = {'s':0,'t':int(time.time())}
	queues = {}
	batchPutPartSize = 25

	def helper(dom, keys):
		batchData = dict((k, bare.copy()) for k in keys)
		for k, v in batchData.iteritems():
			f_ids = random.sample(xrange(N), FN)
			merged = []
			friends = []
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
			v['f'] = friends

		while 1:
			try:
				if dom.batch_put_attributes(batchData, replace=True):
					break
			except boto.exception.SDBResponseError:
				gevent.sleep(0.1)
		cc[0] += len(keys)
		if random.randrange(10)== 0:
			print 'add_user', keys[0],'~',keys[-1], ':', cc[0], time.time() - t1, (time.time()-t1)*N/cc[0]

	def create_user_worker(dom, queue):
		collects = []
		while True:
			item = queue.get()
			if item is None:
				break
			collects.append(item)
			if len(collects)==25:
				tasks.put((helper, dom, collects))
				collects = []
		if collects:
			tasks.put((helper, dom, collects))

	for dom, key in s.db.iter_split_keys_by_domain('%x'% x for x in xrange(N)):
		if dom not in queues:
			queues[dom] = gevent.queue.JoinableQueue(5)
			gevent.spawn(create_user_worker,dom,queues[dom])
		queues[dom].put(key)
		gevent.sleep()
	for queue in queues.itervalues():
		queue.put(None)
	tasks.join()
# normal loop
	#for i in xrange(N):
		#def helper(i):
			#while 1:
				#try:
					#s.add_user(i)
					#break
				#except:
					#gevent.sleep(0.1)
					#continue
			#cc[0] += 1
			#if random.randrange(10) == 0:
				#print 'add_user', i, cc[0], time.time() - t1, (time.time()-t1)*N/cc[0]
		#tasks.put((helper, i))
		#gevent.sleep()
	print time.time() - t1
	s.conn.print_usage()
	raw_input('press enter')
#create_users()


#create_friends()

def update_scores():
	t1 = time.time()
	jobs = []
	cc = [0]
	for i in xrange(N):
		def helper(i):
			r = random.randrange(100)
			ret = s.update_score(i, r)
			cc[0] += 1
			if random.randrange(100) == 0:
				print 'update_score', i,r, ret, time.time()-t1, (time.time()-t1)/ cc[0] * N 
		tasks.put((helper, i))
		gevent.sleep(0)
	tasks.join()
	print time.time() - t1
	s.conn.print_usage()

	raw_input('press enter')
#update_scores()
def get_ranking_test():
	for i in xrange(10000):
		t1 = time.time()
		print 'get_ranking', i, s.get_ranking(None, i)
		print time.time() - t1
		s.conn.print_usage()
	
k = 'c53b8476009bcc1713059c4fc7cedb42fbca052a'
g = '5sec'
#s.put_game(g, 0, 40)
#print s.get_game(g)
#s.put_game(g, 0, 40, k)
#print s.get_game(g)
#s.put_game(g, 0, 30, k)
#print s.get_game(g)
s.put_game(g, 0, 5)
s.add_user(g, 1)
s.add_user(g, 2)
s.add_friends(g, 1, [2])
while 1:
	t = time.time()
	s.update_score(g, 1, random.randrange(100))
	print time.time()-t,'A'
	s.update_score(g, 2, random.randrange(100))
	print time.time()-t,'B'
	print s.get_friend_score_list(g, 1)
	print s.db.get_user_score(g, 1), s.get_ranking(g, 1), s.db.get_user_score(g, 2), time.time()-t
	gevent.sleep(1)
	s.conn.print_usage()
#print s.get_game('test')
s.conn.print_usage()

#d = []
#for i in xrange(10):
	#for j in xrange(10):
		#d.append(s.hash('node_test%d:%d' % (i,j)))
#d.sort()
#print '\n'.join(d)

