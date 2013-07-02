import unittest
import simpledb as db

class ScoresTest(unittest.TestCase):
	def setUp(self):
		self.game = dict(i=30,b=0)
	def testLoadDump(self):
		s = db.load_scores("1 1 1 1")
		self.assertEqual([1]*4,s)
		self.assertEqual("1 1 1 1", db.dump_scores(s))
		self.assertEqual([1, 0], db.load_scores("1"))
	def testMakeNewScore(self):
		self.assertEqual([0]*4,db.make_new_score(self.game, [1,1,1,1], 0,0))
		last_reset_time = db.get_last_reset_time(self.game)
		scores = [10, last_reset_time+15]
		self.assertEqual([10,last_reset_time+15, 0, 0], db.make_new_score(self.game, scores, 0, 0))
		scores = [10, last_reset_time-15]
		self.assertEqual([0,0,10,last_reset_time-15], db.make_new_score(self.game, scores, 0, 0))
		self.assertEqual([0,0,10,last_reset_time-15], db.make_new_score(self.game, scores, 20, last_reset_time-10))
		self.assertEqual([5,last_reset_time+5,10,last_reset_time-15], db.make_new_score(self.game, scores, 5, last_reset_time+5))
		scores += [0,0]
		self.assertEqual([0,0,10,last_reset_time-15], db.make_new_score(self.game, scores, 0, 0))


if __name__ == '__main__':
	unittest.main()
