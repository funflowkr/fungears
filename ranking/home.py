# encoding: utf-8

from flask import Flask, request
import json
import simpledb as db
import string
import time
import config
import urlparse
from werkzeug.exceptions import *
db.init(config.isTest)

app = Flask(__name__)

valid_char = set(string.ascii_letters+string.digits+'_')
def is_valid_game_id(game_id):
	return not set(str(game_id)) - valid_char

def check_game_secret(gameId):
	if request.data:
		request.form = json.loads(request.data)
	secret = request.form['secret']
	g = db.get_game(gameId)
	print g
	if g['k'] != secret:
		raise Forbidden('{"error":"invalid secret"}')
	#assert g['k'] == secret

@app.route('/')
def index():
	return "Hello there"

@app.route('/admin')
def admin_page():
	return "to work"

@app.route('/game/<gameId>', methods=['GET','POST'])
def game_page(gameId):
	print 'hello'
	if request.data:
		request.form = json.loads(request.data)
	if not is_valid_game_id(gameId):
		return json.dumps(dict(error="invalid id", id=gameId)), 403
	if request.method == 'POST':
		resetBase = int(request.form['base'])
		resetInterval = int(request.form['interval'])
		secret = None
		if 'secret' in request.form:
			secret = request.form['secret']
		ret = db.put_game(gameId, resetBase, resetInterval, secret)
		g = db.get_game(gameId)
		if ret:
			return json.dumps(dict(id=gameId, base=g['b'], interval=g['i'], secret=ret)), 201
		else:
			return json.dumps(dict(id=gameId, base=g['b'], interval=g['i'])), 403
	else:
		#elif request.method == 'GET':
		g = db.get_game(gameId)
		print g
		if g:
			return json.dumps(dict(id=gameId, base=g['b'], interval=g['i']))
		else:
			return json.dumps(dict(id=gameId, error="Not found")), 404

#@app.route('/user/<gameId>/<int:userId>', methods=['GET', 'POST'])
#def user_page(gameId, userId):
#	return ''

@app.route('/update_friends/<gameId>/<int:userId>', methods=['POST'])
def update_friends(gameId, userId):
	check_game_secret(gameId)
	friends = request.form['friends']
	ret = db.update_friends(gameId, userId, friends)
	db.update_api_usage(gameId, 'update_friends')
	return json.dumps(dict(success=ret))

@app.route('/friend_scores/<gameId>/<int:userId>', methods=['GET','POST'])
def friend_scores(gameId, userId):
	startTime = time.time()
	check_game_secret(gameId)
	print time.time() - startTime
	score_list = db.get_friend_score_list(gameId, userId)
	score_list.sort(key=lambda x:-x[1])
	ret = json.dumps([[int(x,16), y] for x, y in score_list])
	print time.time() - startTime
	db.update_api_usage(gameId, 'friend_scores')
	return ret

@app.route('/friend_scores_before_last_reset/<gameId>/<int:userId>', methods=['GET','POST'])
def friend_scores_before_last_reset(gameId, userId):
	startTime = time.time()
	check_game_secret(gameId)
	print time.time() - startTime
	score_list = db.get_friend_score_list(gameId, userId, use_score_before_reset = True)
	score_list.sort(key=lambda x:-x[1])
	ret = json.dumps([[int(x,16), y] for x, y in score_list])
	print time.time() - startTime
	db.update_api_usage(gameId, 'friend_scores_before_last_reset')
	return ret

@app.route('/update_score/<gameId>/<int:userId>/<int:score>', methods=['POST'])
def update_score(gameId, userId, score):
	startTime = time.time()
	check_game_secret(gameId)
	forced = request.form.get('forced', False)
	isUpdated, scoreNow = db.update_score(gameId, userId, score, forced)
	print 'UPDATE_SCORE', time.time() - startTime
	db.update_api_usage(gameId, 'update_score')
	return json.dumps(dict(score=scoreNow, updated=isUpdated))

# 쓸모가 없다
#@app.route('/ranking/<gameId>/<int:userId>')
#def ranking(gameId, userId):
#	rank = db.get_ranking(gameId, userId)
#	return json.dumps(dict(rank=rank))

@app.route('/ranking_from/<gameId>/<int:userId>')
def ranking_from(gameId, userId):
	check_game_secret(gameId)
	view_from = request.form['view_from']
	rankings = db.multiple_get_ranking_from(gameId, userId, view_from)
	db.update_api_usage(gameId, 'ranking_from', len(view_from))
	return json.dumps(rankings)

@app.route('/debug_get_scores_and_ranking/<gameId>/<int:userId>')
def debug_get_scores_and_ranking_from(gameId, userId):
	check_game_secret(gameId)
	score_list = db.get_friend_score_list(gameId, userId)
	score_list.sort(key=lambda x:-x[1])
	view_from = [int(x,16) for x, y in score_list]
	rankings = dict(db.multiple_get_ranking_from(gameId, userId, view_from))
	db.update_api_usage(gameId, 'debug_get_scores_and_ranking')
	return json.dumps([[int(x,16), y, rankings[int(x,16)]] for x, y in score_list])

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=80, debug=True)
