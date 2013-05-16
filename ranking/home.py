from flask import Flask, request
import json
import simpledb as db
import string

app = Flask(__name__)

valid_char = string.ascii_letters+string.digits+'_'
def is_valid_game_id(game_id):
	return not game_id.translate(None, valid_char)

def check_game_key(gameId):
	key = request.form['key']
	g = db.get_game(gameId)
	assert g['k'] == key

@app.route('/')
def index():
	return "Hello there"

@app.route('/admin')
def admin_page():
	return "to work"

@app.route('/game/<gameId>', methods=['GET','POST'])
def game_page(gameId):
	if not is_valid_game_id(gameId):
		return json.dumps(dict(error="invalid id", id=gameId)), 400
	if request.method == 'POST':
		raise not db.find_game(gameId):
		resetBase = int(request.form['base'])
		resetInterval = int(request.form['interval'])
		key = None
		if 'key' in request.form:
			key = request.form['key']
		ret = db.put_game(gameId, resetBase, resetInterval, key)
		g = db.get_game(gameId)
		if ret:
			return json.dumps(dict(id=gameId, base=g['b'], interval=g['i'], key=g['k'])), 201
		else:
			return json.dumps(dict(id=gameId, base=g['b'], interval=g['i'])), 403
	else:
		#elif request.method == 'GET':
		g = db.get_game(gameId)
		if g:
			return json.dumps(dict(id=gameId, base=g['b'], interval=g['i']))
		else:
			return json.dumps(dict(id=gameId, error="Not found")), 404

@app.route('/user/<gameId>/<int:userId>', methods=['GET', 'POST'])
def user_page(gameId, userId):
	return ''

@app.route('/update_friends/<gameId>/<int:userId>', methods=['POST'])
def update_friends(gameId, userId):
	check_game_key(gameId)
	friends = request.form['friends']
	friends = [int(x) for x in friends.split(',')]
	ret = db.update_friends(gameId, userId, friends)
	return json.dumps(dict(success=ret))

@app.route('/friend_scores/<gameId>/<int:userId>')
def friend_scores(gameId, userId):
	score_list = db.get_friend_score_list(gameId, userId)
	return json.dumps([[int(x,16), y] for x, y in score_list])

@app.route('/update_score/<gameId>/<int:userId>/<int:score>', method=['POST'])
def update_score(gameId, userId, score):
	isUpdated = db.update_score(gameId, userId, score)
	scoreNow = db.db.get_user_score(gameId, userId)
	return json.dumps(dict(score=scoreNow, updated=isUpdated))

@app.route('/ranking/<gameId>/<int:userId>')
def ranking(gameId, userId):
	return ''

@app.route('/ranking_from/<gameId>/<int:userId>')
def ranking_from(gameId, userId):
	return ''

if __name__ == '__main__':
	app.run()
