# encoding: utf-8

from flask import Flask, request
import json
import simpledb as db
import string
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
	return json.dumps(dict(success=ret))

@app.route('/friend_scores/<gameId>/<int:userId>', methods=['GET'])
def friend_scores(gameId, userId):
	check_game_secret(gameId)
	score_list = db.get_friend_score_list(gameId, userId)
	score_list.sort(key=lambda x:-x[1])
	return json.dumps([[int(x,16), y] for x, y in score_list])

@app.route('/update_score/<gameId>/<int:userId>/<int:score>', methods=['POST'])
def update_score(gameId, userId, score):
	check_game_secret(gameId)
	isUpdated = db.update_score(gameId, userId, score)
	scoreNow = db.db.get_user_score(gameId, userId)
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
	return json.dumps(rankings)

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=80, debug=True)
