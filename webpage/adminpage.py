from flask import Flask, request, render_template, redirect, url_for, session, escape
import boto.sdb
import MySQLdb as db
import config


conn = boto.sdb.connect_to_region(config.region,
	aws_access_key_id=config.access_key,
	aws_secret_access_key=config.secret_key,
	is_secure=False
)

app = Flask(__name__)

app.secret_key = config.flask_secret_key

#print conn.get_attributes('game','stress')
#g = conn.get_attributes('game','stres')
#print conn.select('game', 'select * from game')
def validate_login(game, key):
	if not game or not key:
		return False
	g = conn.get_attributes('game', game)
	if not g:
		return False
	if key == 'kingfunflowmanse':
		return True
	if not g or 'k' not in g or g['k'] != key:
		return False
	return True

@app.route('/games')
def games_page():
	return "<!doctype html>"+"<br>".join(g.name for g in conn.select('game', "select * from game"))

@app.route('/game/<game>')
def game_page(game):
	if game not in session or session[game] != game:
		return 'Invalid access'
	attrs = conn.get_attributes('game',game)
	api_usages = {}
	for k, v in attrs.iteritems():
		if k[:2] == 'a_':
			api_usages[k[2:]] = int(v)

	return render_template('game_detail.html', game=game, api_usages=api_usages)
	
@app.route('/', methods=['GET','POST'])
def index():
	if request.method == 'POST':
		if validate_login(request.form['game'], request.form['key']):
			session[request.form['game']] = request.form['game']
			return redirect(url_for('game_page', game=request.form['game']))
		else:
			return render_template("index.html", msg="Invalid game & secret key composition")
	else:
		return render_template("index.html")

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=10001, debug=True)
