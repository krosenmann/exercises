#!/usr/bin/env python3.6
import httplib2
import hashlib
import redis
from sqlalchemy import create_engine, Column, Integer, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


ht = httplib2.Http('.cache')


def request(app_id, country_id):
    response, content = ht.request(
	f'https://itunes.apple.com/us/app/id{app_id}',
	headers={'User-Agent': 'AppStore/2.0 iOS/8.4 model/iPhone4,1 build/12H143 (6; dt:73)',
		 'X-Apple-Store-Front': f'{country_id},29 t:native',}
    )
    return response.fromcache, content


def check_changes(resp_body, app_id, country_id):
    hashed_body = hashlib.md5(resp_body)
    message = hashed_body.hexdigest()
    key = f'{app_id}+{counry_id}'
    r = redis.Redis(host='localhost', port=6379, db=0)
    last_info_hash = r.get(key)
    if last_info_hash == message:
	return
    r.set(key, message)
    return hashed_body.hexdigest()
def configure(conf='parser.conf'):
    config = configparser.ConfigParser()
    config.read(conf)
    return {k: v for k, v in config['postgres'].items()}


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--app_id", help="AppStore application ID",
			default='284882215')
    parser.add_argument("--country_id", help="AppStopre country ID",
			default='143469-16')
    args = parser.parse_args()
    return args.__dict__




from contextlib import contextmanager


Base = declarative_base()
conf = configure()
engine = create_engine(f"postgres://{conf['user']}:{conf['password']}@{conf['host']}:{conf['port']}/testdb")
Session = sessionmaker(bind=engine)


@contextmanager
def session_scope():
    session = Session()
    try:
	yield session
	session.commit()
    except:
	session.rollback()
	raise
    finally:
	session.close()




class AppInfo(Base):
    __tablename__ = 'app_info'

    id = Column(Integer, primary_key=True)
    country = Column(String)
    app = Column(String)
    data = Columnt(Text)


def write_data_to_db(load_date, country_id, app_id, data_blob):
    with session_scope() as session:
	app_info=AppInfo(load_date=load_date, 
			 country=contry_id,
			 app=app_id,
			 data=data_blob)
	session.add(app_info)
	session.commit()




def main():
    from datetime import datetime
    dbconf = configure()
    parser_args = parse_arguments()
    fromc, content = request(**parser_args)
    load_date = datetime.now().isoformat()
    if fromc:
	return
    if not check_changes(content, **parser_args):
	return
    write_data_to_db(load_date, data_blob=content, **parser_args)
    return

if __name__ == '__main__':
    main()
