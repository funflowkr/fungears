import MySQLdb as db
import boto.sdb
import config
import json
dbaddr = 'rankingstatdb.cjiw5rpe2jfd.ap-northeast-1.rds.amazonaws.com'

conn = boto.sdb.connect_to_region(config.region,
	aws_access_key_id=config.access_key,
	aws_secret_access_key=config.secret_key,
	is_secure=False
)

dbconn = db.connect(config.dbaddr, user=config.dbuser, passwd=config.dbpasswd, db=config.dbname)
cursor = dbconn.cursor()
#print cursor.execute('delete from  ranking_stat')
#print cursor.fetchall()
#cursor.execute('drop table ranking_stat')
#cursor.execute('create table ranking_stat (id int primary key auto_increment, whenadded datetime, gamename varchar(250) not null, body text)')
params = []
for item in conn.select('game_test', 'select * from game_test'):
	api_usages = {}
	for key, value in item.iteritems():
		if key[:2] == 'a_':
			api_usages[key[2:]] = value
	if api_usages:
		params.append(('_'+str(item.name), json.dumps(api_usages)))
for item in conn.select('game', 'select * from game'):
#for item in conn.select('game_test', 'select * from game_test'):
	api_usages = {}
	for key, value in item.iteritems():
		if key[:2] == 'a_':
			api_usages[key[2:]] = value
	if api_usages:
		params.append((str(item.name), json.dumps(api_usages)))
cursor.executemany('insert into ranking_stat(whenadded, gamename, body) values(now(), %s, %s)', params)
dbconn.commit()
